var debug = require('debug')('signal-reindex');
var through = require('through2');
var signal = require( './index' )

module.exports = SignalElasticReindex;
module.exports.stream = SignalElasticReindexStream;

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
function SignalElasticReindex ( options, callback ) {
  return signal( 'siteSearchReindex' )( options, callback )
}

/**
 * Send a build signal for the site. First configure with
 * a Firebase reference, then signal with a sitename.
 */
function SignalElasticReindexStream () {
  if ( ! ( this instanceof SignalElasticReindexStream ) ) return new SignalElasticReindexStream()

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
      SignalElasticReindex( signalOptions, function ( error ) {
        if ( error ) return next( error )
        next( null, row )
      } )
    } )
  }

}