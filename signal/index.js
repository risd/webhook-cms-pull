var debug = require( 'debug' )( 'signal' )
var assert = require( 'assert' )

var firebaseEscape = require( '../util/firebase-escape.js' )

module.exports = Signal;

function Signal( signalType ) {
  if ( ! ( this instanceof Signal ) ) return new Signal( signalType )
  assert( typeof signalType === 'string', 'Signal expects a string telling the type of signal to send.' )
  
  return function SendSignal ( options, callback ) {
    if ( ! ( this instanceof SendSignal ) ) return new SendSignal( options, callback )
    assert( typeof options === 'object', 'Options object is required to send signal. Including a .firebase Firebase instance, and .payload.sitename string.' )
    assert( typeof options.firebase === 'object', 'options.firebase must be a Firebase instance.' )
    assert( typeof options.payload === 'object', 'options.payload must be an object with .sitename string.' )
    assert( typeof options.payload.sitename === 'string', 'options.payload must be an object with .sitename string.' )

    debug( 'send' )
    debug( signalType )

    try {
      var commandReference = options.firebase.root.child( 'management/commands/' + signalType )

      var payload = Object.assign( {
        id: uniqueId(),
      }, options.payload )

      payload.sitename = firebaseEscape( payload.sitename )

      commandReference.child( payload.sitename ).set( payload, function ( error ) {
        debug( 'send:done' )
        debug( error )

        if ( error ) return callback( error )
        else return callback( null, payload )
      } )

    } catch( error ) {
      debug( 'send:done' )
      debug( error )
      
      callback( error )
    }

  }
}

function uniqueId() {
  return Date.now() + 'xxxxxxxxxxxx4xxxyxxxxxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    var r = Math.random()*16|0, v = c === 'x' ? r : (r&0x3|0x8);
    return v.toString(16);
  });
}
