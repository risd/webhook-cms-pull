var debug = require('debug')('signal-build');
var through = require('through2');
var signal = require( './index' )

module.exports = SignalBuild;
module.exports.stream = SignalBuildStream;

/**
 * Send a build signal for the site.
 * 
 * @param {object}   options
 * @param {object}   options.firebase
 * @param {object}   options.payload
 * @param {string}   options.payload.siteName
 * @param {string}   options.payload.userid?
 * @param {Function} callback
 */
function SignalBuild ( options, callback ) {
  return signal( 'build' )( options, callback )
}

/**
 * Send a build signal for the site. First configure with
 * a Firebase reference, then signal with a sitename.
 */
function SignalBuildStream () {
  if ( ! ( this instanceof SignalBuildStream ) ) return new SignalBuildStream()

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
      SignalBuild( signalOptions, function ( error ) {
        if ( error ) return next( error )
        next( null, row )
      } )
    } )
  }

}