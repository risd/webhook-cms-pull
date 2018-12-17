#!/usr/bin/env node

var debug = require('debug')('sync:main');
var fs = require('fs');
var through = require('through2');
var pump = require('pump');

module.exports = Sync;

/**
 * Create a sync instance with the options passed in.
 * 
 * @param {object}   options
 * @param {object}   options.sourcePrototype  The pull source object to be extended with the sync protocol
 * @param {object}   options.syncNode  The name of the node to use when syncing sources
 * @param {object}   options.env  The object that contains environment configuration
 * @param {object}   options.env.firebase  The Firebase environment configuration
 * @param {string}   options.env.firebase.firebaseName  Firebase database name
 * @param {string}   options.env.firebase.firebaseKey  Firebase database API key
 * @param {string}   options.env.firebase.siteName  Firebase database site name
 * @param {string}   options.env.firebase.siteKey  Firebase database site key
 * @param {object}   options.env.signal.payload  Base object to extend when submitting a server signal
 * @param {string}   options.env.signal.payload.userid  User responsible for the signal
 * @param {string}   options.env.signal.payload.sitename  Signal for a the given site name
 * @param {object}   options.env.aws  AWS environment configuration
 * @param {string}   options.env.aws.key  AWS key
 * @param {string}   options.env.aws.secret  AWS secret
 * @param {string}   options.env.aws.bucket  AWS bucket to report sync progress to
 * @param {Function} callback  Function called upon sync completion
 */
function Sync ( options, callback ) {
  if ( ! ( this instanceof Sync ) ) return new Sync( options, callback )

  if ( typeof options !== 'object' ) {
    throw new Error( 'An options object must be passed in. Including a `sourcePrototype` & `syncNode` string.' )
  }

  var envConf = options.env;
  var syncNode = options.syncNode || 'sync';

  // this doesn't have to be a stream, but could be.
  var Firebaseref = require('./firebaseref.js');

  var sourcePrototype = options.sourcePrototype;
  if ( ! sourcePrototype ) {
    throw new Error( 'Sync requires a source prototype object to extend with the sync protocol.' );
  }
  var sourcePrototypes = [ sourcePrototype ];

  var SignalBuild = require( './signal/build.js' ).stream();
  var SignalReindex = require( './signal/elastic-reindex.js' ).stream();
  var Report = require('./report/index.js')( envConf.report );

  var syncRoot = timestampWithPrefix( syncNode );

  // Pass firebase ref into objects
  Firebaseref( envConf.firebase )
    // < Reads a firebase ref
    // > Writes a firebase ref
    .pipe(AddSyncNodeToFirebase(syncRoot))
    .pipe(SignalBuild.config())
    .pipe(SignalReindex.config())
    .pipe(Report.config())
    // < Reads a firebase ref
    // > Writes individual SyncSources with sync protocol applied
    .pipe(ApplySyncProtocolToSources(syncRoot, sourcePrototypes))
    // < Reads SyncSource
    // > Writes SyncSource
    .pipe(GetSourcesData())
    .pipe(AddSourceToWebhook())
    .pipe(RemoveFromWebhookBasedOnSource())
    .pipe(ResolveRelationships())
    .pipe(ResolveReverseRelationships())
    .pipe(Report.update())
    .pipe(SignalBuild.send( envConf.signal ))
    .pipe(SignalReindex.send( envConf.signal ))
    // < Reads SyncSources
    // > Writes nothing
    .pipe(Exit( callback ));

  function timestampWithPrefix (prefix) {
    var now = new Date();
    var timestamp = now.toISOString()
      .replace(/:/g, '-')
      .replace(/T/g, '--')
      .split('.')[0];
    return [
        prefix,
        timestamp,
      ].join('--');
  }


  function AddSyncNodeToFirebase (syncRoot) {
    return through.obj(addEduSync);

    function addEduSync (fb, enc, next) {
      debug('Add eduSync to Firebase.');

      fb.child(syncRoot)
        .set({}, onComplete);

      function onComplete (error) {
        if (error) {
          throw new Error(error);
        }
        next(null, fb);
      }
    }
  }

  /**
   * Returns a stream that expects a firebase reference
   * (fb), and pushes into the stream Sync protocol
   * compatabile objects.
   * 
   * @param {string} syncNodeName       String of the node name
   *                                    to use for syncing new data 
   * @param {object} sourcePrototypes[] Array of 
   */
  function ApplySyncProtocolToSources (syncNodeName, sourcePrototypes) {
    var SyncProtocol = require('./sync-protocol.js');

    return through.obj(fbref);

    function fbref (fb, enc, next) {
      debug('Applying sync protocol to sources.');

      var stream = this;

      sourcePrototypes
        .map(configure)
        .forEach(pushInto(stream));

      this.push(null);
      next();

      function configure (source) {
        SyncProtocol(syncNodeName, source, fb);
        return source( envConf );
      }
      function pushInto (stream) {
        return function pusher (source) {
          source.errors = [];
          stream.push(source);
        };
      }
    }
  }

  function counter () {
    var c = 0;

    return {
      stream: function () {
        return through.obj(function (row, enc, next) {
          c += 1;
          this.push(row);
          next();
        });
      },
      count: function () {
        return c;
      }
    };
  }

  function GetSourcesData () {
    return through.obj(getSourceData);

    function getSourceData (source, enc, next) {
      debug(
        'Get all data from source & put into firebase.');

      var c = counter();

      if (('listSource' in source) &&
          ('sourceStreamToFirebaseSource' in source)) {

        pump(
          source.listSource(),
          c.stream(),
          source.sourceStreamToFirebaseSource(),
          end);
      }
      else {
        end(true);
      }

      function end (err) {
        if (err) debug('get-source-data:end', err);
        else debug('get-source-data:end', c.count());
        
        if (c.count() === 0) {
          var NoSoureData = new Error('No source data. Sync stopped.');
          source.errors.push(NoSoureData);
        }
        
        var stepError = new Error('Could not get source data.');
        if (err) {
          source.errors
            .push(stepError);

          if (err instanceof Error) {
            source.errors.push(err);
          }
        }
        next(null, source);
      }
    }
  }

  function Sink () {
    return through.obj( function (row, enc, next ) {
      next();
    });
  }

  function AddSourceToWebhook () {
    return through.obj(addSource);

    function addSource (source, enc, next) {
      debug('Add source data to webhook data.');

      if ((source.errors.length === 0) &&
          ('listFirebaseSource' in source) &&
          ('addSourceToWebhook' in source)) {
        pump(
          source.listFirebaseSource(),
          source.addSourceToWebhook(),
          Sink(),
          end);
      }
      else {
        end(true);
      }

      function end (err) {
        var stepError = new Error(
          'Could not add source data to webhook data');
        if (err) {
          source.errors
            .push(stepError);

          if (err instanceof Error) {
            source.errors.push(err);
          }
        }
        next(null, source);
      }
    }
  }

  function RemoveFromWebhookBasedOnSource () {
    /**
     * List the webhook and source values
     * if the webhook value is not in the
     * source values, invoke the model's
     * `webhookValueNotInSource`
     */
    return through.obj(remove);

    function remove (source, enc, next) {
      debug('Remove from WebHook based on source.');
      
      if ((source.errors.length === 0) &&
          ('listFirebaseWebhook' in source) &&
          ('addInSourceBool' in source) &&
          ('updateWebhookValueNotInSource' in source)) {
        pump(
          source.listFirebaseWebhook(),
          source.addInSourceBool(),
          source.updateWebhookValueNotInSource(),
          Sink(),
          end);
      }
      else {
        end(true);
      }

      function end (err) {
        var stepError = new Error(
          'Could not remove from webhook based on source.');
        if (err) {
          source.errors
            .push(stepError);

          if (err instanceof Error) {
            source.errors.push(err);
          }
        }
        next(null, source);
      }
    }
  }

  function ResolveRelationships () {
    return through.obj(relationships);

    function relationships (source, enc, next) {
      debug('Resolve Relationships.');

      if ((source.errors.length === 0) &&
          ('rrListWebhookWithRelationshipsToResolve' in source) &&
          ('rrGetRelatedData' in source) &&
          ('rrResetRelated' in source) &&
          ('rrEnsureReverseRootNode' in source) &&
          ('rrEnsureReverseContenTypeNode' in source) &&
          ('rrEnsureReverseContentTypeValueNode' in source) &&
          ('rrEnsureReverseKeyNode' in source) &&
          ('rrPopulateRelated' in source) &&
          ('rrSaveReverse' in source) &&
          ('rrSaveCurrent' in source)) {
        pump(
          source.rrListWebhookWithRelationshipsToResolve(),
          source.rrGetRelatedData(),
          source.rrResetRelated(),
          source.rrEnsureReverseRootNode(),
          source.rrEnsureReverseContenTypeNode(),
          source.rrEnsureReverseContentTypeValueNode(),
          source.rrEnsureReverseKeyNode(),
          source.rrPopulateRelated(),
          source.rrSaveReverse(),
          source.rrSaveCurrent(),
          end);
      }
      else {
        end(true);
      }

      function end (err) {
        var stepError = new Error(
          'Could not resovle relationships.');
        if (err) {
          source.errors
            .push(stepError);

          if (err instanceof Error) {
            source.errors.push(err);
          }
        }
        next(null, source);
      }
    }
  }

  function ResolveReverseRelationships () {
    return through.obj(reverse);

    function reverse (source, enc, next) {

      if ((source.errors.length === 0) &&
          ('rrrListRelationshipsToResolve' in source) &&
          ('rrrAddData' in source) &&
          ('rrrFormatData' in source) &&
          ('rrrSave' in source)) {
        pump(
          source.rrrListRelationshipsToResolve(),
          source.rrrAddData(),
          source.rrrFormatData(),
          source.rrrSave(),
          end);
      }
      else {
        end(true);
      }


      function end (err) {
        var stepError = new Error(
          'Could not resovle relationships.');
        if (err) {
          source.errors
            .push(stepError);

          if (err instanceof Error) {
            source.errors.push(err);
          }
        }
        next(null, source);
      }
    }
  }

  function Exit ( callback ) {
      // Firebase sync node reference
      // that should be set to null
      // on complete
      var syncRoot;

      return through.obj(sink, end);

      function sink (source, enc, next) {
        syncRoot = source._firebase.syncRoot;
        next();
      }

      function end () {
        try {
          syncRoot.set(null, function () {
            callback();
          });
        }
        catch (err) {
          callback(err);
        }
      }
  }

}