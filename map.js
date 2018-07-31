var debug = require( 'debug' )( 'map-coordinator' )
var parallelLimit = require( 'run-parallel-limit' )
var FirebaseRef = require( './firebaseref.js' )
var Signal = require( './signal/index' )
var EventEmitter = require( 'events' )

var MAX_CONCURRENCY = 10;

module.exports = MapCoordinator;
module.exports.protocol = MapProtocol;

/**
 * Apply mapFn across objects defined in the
 * mapPrototype, using the firebaseref as the 
 * source of data.
 * 
 * @param {object} options
 * @param {object} options.mapPrototype
 * @param {object} options.firebase      Options object to initialize firebase.
 * @param {object} options.signal        Options object to signal the server.
 * @param {function} complete The function called upon completing the Map
 */
function MapCoordinator ( options, complete ) {
  if ( ! ( this instanceof MapCoordinator ) ) return new MapCoordinator( options, complete )
  var self = this;

  // save reference for coordination
  this.mapPrototype = MapProtocol( options.mapPrototype )
  
  FirebaseRef( options.firebase, function withFirebaseRef ( error, firebaseref ) {
    if ( error ) return callback( error )

    self.firebaseref = firebaseref;

    // coordination
    self.isOneOff( function ( error, isOneOff ) {
      if ( error ) return complete( error )

      // function used to apply the Map
      var applyMapFn = self.applyMapFn( isOneOff )
      
      // apply the a map fn on the mapPrototype
      self.applyMap( applyMapFn, function ( error, mappedData ) {
        if ( error ) return complete( error )

        // is there a mapRelatedFn defined? if so, lets use it to updated related data
        if ( typeof self.mapPrototype.mapRelatedFn !== 'function' ) return complete( null, { data: mappedData } )

        // get related content types to map
        self.relatedContentTypeKeyPaths( function ( error, relatedContentTypeKeyPaths ) {
          if ( error ) return complete( error )

          // get data for related content types
          self.dataForContentTypeKeyPaths( relatedContentTypeKeyPaths, function ( error, absoluteControlKeyPathData ) {
            if ( error ) return complete( error )

            // apply the related map fn
            var mappedDataKeyPathPairs = self.applyMapRelatedFn( absoluteControlKeyPathData, mappedData )

            // save the related data
            self.saveRelatedControls( mappedDataKeyPathPairs, function ( error, relatedControls ) {
              if ( error ) return complete( error )

              debug( 'signal-reindex' )
              var signalOptions = Object.assign( { firebase: self.firebaseref }, options.signal )
              Signal( 'siteSearchReindex' )( signalOptions, signalComplete )

              function signalComplete ( error, payload ) {
                if ( error ) return complete( error )
                complete( null, { data: mappedData, related: relatedControls }  )
              }
            } )
          } )
        } )
      } )
    } )

  } )
}

MapCoordinator.prototype.isOneOff = function ( complete ) {
  this.firebaseref.child( 'contentType' ).child( this.mapPrototype.webhookContentType ).child( 'oneOff' )
    .once( 'value', oneOffSnapshot, oneOffSnapshotError  )

  function oneOffSnapshot ( snapshot ) {
    var isOneOff = snapshot.val()
    complete( null, isOneOff )
  }

  function oneOffSnapshotError ( error ) {
    complete( error )
  }
}

/**
 * Returns function that can be used to apply the mapPrototype's mapFn.
 * @param  {Boolean} isOneOff Is the shapshot going to be in the shape of oneOff data, or multiple data.
 *                            oneOff data is a single object. multiple data is an object containing keys
 *                            to instance values.
 * @return {Function} applyMapFn ( snapshotData ) -> mappedSnapshotData
 */
MapCoordinator.prototype.applyMapFn = function ( isOneOff ) {
  var self = this;

  return isOneOff ? oneOffMap : multipleMap;

  function oneOffMap ( snapshotData ) {
    return self.mapPrototype.mapFn( snapshotData )
  }

  function multipleMap ( snapshotData ) {
    var mappedData = {};

    function applyMap ( dataKey ) {
      mappedData[ dataKey ] = self.mapPrototype.mapFn( snapshotData[ dataKey ] )
    }

    Object.keys( snapshotData ).forEach( applyMap )

    return mappedData;
  }
}

MapCoordinator.prototype.applyMap = function ( applyMapFn, complete ) {
  var firebaseDataRef = this.firebaseref.child( 'data' ).child( this.mapPrototype.webhookContentType )
  
  debug( 'apply-map' )

  firebaseDataRef
    .once( 'value', function ( snapshot ) {
      var snapshotData = snapshot.val()

      var mappedSnapshotData = applyMapFn( snapshotData )

      debug( 'apply-map:set' )

      firebaseDataRef.set( mappedSnapshotData, setComplete )

      function setComplete ( error ) {
        debug( 'apply-map:set:complete' )
        debug( error )
        complete( error, mappedSnapshotData )
      }
    } )
}

/**
 * () => [ ContentTypeKeyPathControl ]
 *
 * ContentTypeKeyPathControl : { keyPath : ContentTypeKeyPath, control: ContentTypeControl }
 *
 * ContentTypeKeyPath : [ type, widget ]
 *  | [ type, widget, [], grid-key ]
 *  | [ type, {}, widget ]
 *  | [ type, {}, widget, [], grid-key ]
 *  
 * @param  {Function} complete Callback
 */
MapCoordinator.prototype.relatedContentTypeKeyPaths = function ( complete ) {
  debug( 'related-content-type-key-paths' )
  var siteDataRef = this.firebaseref.child( 'data' )
  var mappedContentType = this.mapPrototype.webhookContentType;

  this.firebaseref.child( 'contentType' )
    .once( 'value', contentTypeSnapshot, contentTypeError )

  function contentTypeSnapshot ( snapshot ) {
    var contentTypes = snapshot.val()
    var relationships = relationshipsForContentType( mappedContentType, contentTypes )
    var relationshipKeyPaths = relationships.map( keepKeys( 'keyPath' ) )
    complete( null, relationshipKeyPaths )
  }

  function contentTypeError ( error ) {
    complete( error )
  }

  function relationshipsForContentType ( relatedToContentType, contentTypes ) {
    var isRelatedControl = function ( control ) {
      return ( control.controlType === 'relation' && control.meta.contentTypeId === relatedToContentType )
    }

    var keysControls = contentTypeKeysControls( contentTypes )
    var relationships = keysControls.filter( controlPasses( isRelatedControl ) )

    return relationships;

    function controlPasses ( predicate ) {
      return function ( keyPathControlPair ) {
        return predicate( keyPathControlPair.control )
      }
    }
  }

  /**
   * ContentTypes => [ ContentTypeKeyPathControl ]
   *
   * ContentTypeControl : { name : string, controlType: string, controls : [ ContentTypeControl ]? }
   *
   * ContentTypeKeyPathControl : { keyPath : ContentTypeKeyPath, control: ContentTypeControl }
   * 
   * @param  {object} contentTypes  The webhook contentType object for the site
   * @return {object} pairs         Array of ContentTypeKeyPathControl
   */
  function contentTypeKeysControls ( contentTypes ) {
    var pairs = [];

    function addPair ( keyPath, control ) {
      pairs.push( {
        keyPath: keyPath.slice( 0 ),
        control: Object.assign( {}, control ),
      } )
    }

    Object.keys( contentTypes ).forEach( function ( contentTypeKey ) {
      var contentType = contentTypes[ contentTypeKey ]
      var contentTypeKeyPath = [ contentTypeKey ]

      if ( contentType.oneOff === false ) contentTypeKeyPath.push( {} )

      contentType.controls.forEach( function ( control ) {
        var keyPath = contentTypeKeyPath.concat( [ control.name ] )

        if ( control.controlType === 'grid' ) {
          keyPath.push( [] )

          control.controls.forEach( function ( gridControl ) {
            var gridControlKeyPath = keyPath.concat( [ gridControl.name ] )
            addPair( gridControlKeyPath, gridControl )
          } )
        }
        else {
          addPair( keyPath, control )
        }
      } )
    } )

    return pairs;
  }

  function keepKeys ( keys ) {
    if ( typeof keys === 'string' ) keys = [ keys ]
    return function ( value ) {
      var keep = {}
      keys.forEach( function ( key ) { keep[ key ] = value[ key ] } )
      return keep;
    }
  }
}

/**
 * ( contentTypeKeyPaths : [ { keyPath : ContentTypeKeyPath } ] ) => [ AbsoluteControlKeyPathData ]
 *
 * AbsoluteControlKeyPathData : { keyPath : ContentTypeKeyPath, control : ControlData }
 *
 *   Where `control` is the value of the `control-key` of the ContentTypeKeyPath
 *
 * ContentTypeKeyPath : [ type, control-key ]
 *   | [ type, control-key, [], grid-key ]
 *   | [ type, item-key, control-key ]
 *   | [ type, item-key, control-key, [], grid-key ]
 *
 * ControlData : ( {} | [] | '' )
 * 
 * @param  {[type]} controlKeyPath
 * @param  {[type]} complete       Function to call with 
 */
MapCoordinator.prototype.dataForContentTypeKeyPaths = function ( contentTypeKeyPaths, complete ) {
  debug( 'data-for-content-type-key-paths' )

  var siteDataRef = this.firebaseref.child( 'data' )

  var dataTasks = contentTypeKeyPaths.map( toAbsoluteControlKeyPathTask )

  return parallelLimit( dataTasks, MAX_CONCURRENCY, reduceResults( complete ) )

  function toAbsoluteControlKeyPathTask ( contentTypeKeyPath ) {
    var keyPath = contentTypeKeyPath.keyPath;
    var contentType = keyPath[ 0 ];
    var snapshotToAbsoluteControlKeyPathData = keyPath.filter( isMultipleKeyPath ).length > 0
      ? onMultipleTypeKeySnapshot( keyPath )
      : onSingleTypeKeySnapshot( keyPath );

    return function task ( taskComplete ) {
      siteDataRef.child( contentType ).once( 'value', onSnapshot, onError )

      function onSnapshot ( snapshot ) {
        var absoluteControlKeyPathData = snapshotToAbsoluteControlKeyPathData( snapshot )
        taskComplete( null, absoluteControlKeyPathData )
      }

      function onError ( error ) {
        taskComplete( error )
      }
    }
  }

  function onMultipleTypeKeySnapshot ( contentTypeKeyPath ) {
    var controlKeys = controlKeysFrom( contentTypeKeyPath )

    return function ( snapshot ) {
      var multiple = snapshot.val()

      var absoluteControlKeyPathData = Object.keys( multiple ).map( multipleKeyToAbsoluteControlKeyPathData )

      function multipleKeyToAbsoluteControlKeyPathData ( key ) {
        return {
          keyPath: replaceInArray( contentTypeKeyPath, 1, key ),
          control: multiple[ key ][ controlKeys.control ],
        }
      }

      return absoluteControlKeyPathData;
    }
  }

  function onSingleTypeKeySnapshot ( contentTypeKeyPath ) {
    var controlKeys = controlKeysFrom( contentTypeKeyPath )

    return function ( snapshot ) {
      var single = snapshot.val()

      var absoluteControlKeyPathData = [ {
        keyPath: contentTypeKeyPath,
        control: single[ controlKeys.control ],
      } ]

      return absoluteControlKeyPathData;
    }
  }

  function replaceInArray ( arr, index, value ) {
    return arr.slice( 0, index ).concat( [ value ] ).concat( arr.slice( index + 1 ) )
  }

  function reduceResults ( onReduced ) {
    // results : [ [ {}, {} ], [ {} ] ] => reduced : [ {}, {}, {} ]
    return function ( error, results ) {
      if ( error ) return onReduced( error )
      var reduced = results.reduce( function concat ( previous, current ) { return previous.concat( current ) }, [] )
      onReduced( null, reduced )
    }
  }
}

/**
 * ( controlKeyPathDataArray : [ AbsoluteControlKeyPathData ], mappedData ) => ( mappedControlKeyPathData : [ RelativeControlKeyPathData ] )
 *
 * RelativeControlKeyPath : [ type, control-key ]
 *   | [ type, item-key, control-key ]
 *   
 * ControlData : ( {} | [] | '' )
 *
 * RelativeControlKeyPathData : { keyPath: RelativeControlKeyPath, control: ControlData }
 * 
 * @param  {object} controlKeyPathDataArray  The AbsoluteControlKeyPathData's to apply the `mapRelatedExhibitionsFn` on.
 * @param  {object} mappedData               The data that resulted from `applyMapFn`
 * @return {object} mappedDataKeyPathPairs   The result of the appplication of `mapRelatedExhibitionsFn` on `controlKeyPathDataArray`
 */
MapCoordinator.prototype.applyMapRelatedFn = function ( controlKeyPathDataArray, mappedData ) {
  debug( 'apply-map-related-fn' )
  return controlKeyPathDataArray.map( toMappedDataKeyPathPairs( this.mapPrototype.mapRelatedFn ) )

  function toMappedDataKeyPathPairs ( mapRelatedFn ) {
    return function ( controlKeyPathData ) {
      var control = controlKeyPathData.control;
      var controlKeys = controlKeysFrom( controlKeyPathData.keyPath );
      var controlKey = controlKeys.grid ? controlKeys.grid : controlKeys.control;
      var controlKeyInGrid = typeof controlKeys.grid === 'string' ? true : false;

      return {
        keyPath: controlKeyPathData.keyPath,
        control: mapRelatedFn( control, controlKey, controlKeyInGrid, mappedData )
      }
    }
  }
}

/**
 *
 * controlKeyPathDataArray => controlKeyPathDataArray
 * 
 * controlKeyPathDataArray : [ RelativeControlKeyPathData ]
 *
 * `controlKeyPathDataArray` control's are saved to their keys.
 * The callback `complete` is invoked with the same input data,
 * `controlKeyPathDataArray`
 * 
 * @param  {object} controlKeyPathDataArray
 * @param  {Function} complete              The function to call upon saving `controlKeyPathDataArray`
 */
MapCoordinator.prototype.saveRelatedControls = function ( controlKeyPathDataArray, complete ) {
  debug( 'save-related-controls' )
  var siteDataRef = this.firebaseref.child( 'data' )

  var saveTasks = controlKeyPathDataArray.map( toSaveTasks )

  return parallelLimit( saveTasks, MAX_CONCURRENCY, complete )

  function toSaveTasks ( controlKeyPathData ) {
    var control = controlKeyPathData.control;
    var keyPath = controlKeyPathData.keyPath;

    var controlKeys = controlKeysFrom( keyPath )

    var saveControlKeyPath = controlKeys.grid
      ? keyPath.slice( 0, -2 )
      : keyPath.slice( 0 )

    return function saveTask ( taskComplete ) {
      var controlRef;

      function setControlRefPath ( key ) {
        if ( ! controlRef ) controlRef = siteDataRef.child( key )
        else controlRef = controlRef.child( key )
      }

      saveControlKeyPath.forEach( setControlRefPath )

      // the control does not exist on the reverse relationship, lets skip it
      if ( typeof control === 'undefined') return taskComplete( null, controlKeyPathData )

      controlRef.set( control, onSet )

      function onSet( error ) {
        if ( error ) return taskComplete( error )
        taskComplete( null, controlKeyPathData )
      }
      
    }
  }
}

/* ---- keyPath utils ---- */

function isMultipleKeyPath ( key ) { return typeof key === 'object' && ! Array.isArray( key ) }
function isGridKeyPath ( key ) { return Array.isArray( key ) }

function controlKeysFrom ( contentTypeKeyPath ) {
  var gridPathKeys = contentTypeKeyPath.filter( isGridKeyPath );
  var keyPathContainsGrid = gridPathKeys.length > 0

  var controlKey = keyPathContainsGrid
    ? contentTypeKeyPath.slice( -3, -2 )[ 0 ]
    : contentTypeKeyPath.slice( -1 )[ 0 ];

  var gridKey = keyPathContainsGrid
    ? contentTypeKeyPath.slice( -1 )[ 0 ]
    : undefined;

  return {
    control: controlKey,
    grid: gridKey,
  }
}

/* ---- keyPath utils ---- */

/**
 * Enforces MapProtocol on model.
 * These objects are consumed by the Map function.
 *
 * Must have:
 *
 * .webhookContentType A string that defines the content type
 * .mapFn              A function the defines the 
 * 
 * @param {Function} model The prototype that describes the Map functionality
 */
function MapProtocol ( model ) {

  var enforcer = ErrorMessageAggregator( 'Model does not conform to Map protocol.' )
  
  enforcer.test( attributeTest( 'webhookContentType', 'string'), 'Requires property webhookContentType with string value that represents the webhook content type.' )
  enforcer.test( attributeTest( 'mapFn', 'function' ), 'Requires property mapFn with function value that defines how webhook items get mapped.' )

  enforcer.throw()

  return model()

  function attributeTest( nameOfAttribute, typeOfAttribute, message ) {
    return typeof model.prototype[ nameOfAttribute ] !== typeOfAttribute
  }

  function ErrorMessageAggregator ( baseMessage ) {
    var aggregateMessage = [];

    if ( typeof baseMessage === 'string' ) aggregateMessage.push( baseMessage )

    var messageCountBaseline = aggregateMessage.length;

    function includesMoreThanBase () {
      return aggregateMessage.length > messageCountBaseline;
    }
    
    function addErrorMessage ( errorMessage ) {
      if ( typeof errorMessage === 'string' ) aggregateMessage.push( errorMessage )
    }

    function addIf ( testResult, errorMessage ) {
      if ( testResult ) addErrorMessage( errorMessage )
    }

    function throwError () {
      if ( includesMoreThanBase() ) throw new Error( aggregateMessage )
    }

    return {
      test: addIf,
      throw: throwError,
    }
  }
}
