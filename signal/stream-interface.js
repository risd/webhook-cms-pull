var debug = require('debug')('signal-stream-interface');
var through = require('through2')

module.exports = StreamInterface

/**
 * StreamInterface
 * 
 * expects a signalFn : (options, callback)
 * 
 * returns an object with a stream at the key `config`
 * to configure with firebase database reference. and a
 * stream at the key `send` that will use the `signalFn`
 * to produce the build signal in the firebase command node.
 * 
 * {
 *   config : firebaseRef => firebaseRef,
 *   send : {} => {},
 * }
 *
 * `config` is a pass through stream that expects a firebase database
 * reference to be passed in, and it stores a reference to it.
 *
 * `send` is a pass through stream that will use its stored firebase
 * database reference to send its signal to the firebase commands node.
 */
function StreamInterface (signalFn) {

  var firebase;

  return {
    config: config,
    send  : send,
  }

  function config () {
    return through.obj( function ( firebaseRef, enc, next ) {
      firebase = firebaseRef;
      next( null, firebaseRef );
    } )
  }

  /**
   * @param {object}   options
   * @param {object}   options.payload
   * @param {string}   options.payload.siteName
   * @param {string}   options.payload.userid?
   */
  function send ( options ) {
    return through.obj( function ( row, enc, next ) {
      debug( 'send-from-stream' )
      var signalOptions = Object.assign( options, { firebase: firebase } )
      signalFn( signalOptions, function ( error ) {
        if ( error ) return next( error )
        next( null, row )
      } )
    } )
  }

}
