var debug = require('debug')('signal-reindex');
var signal = require( './index' )
var streamInterface = require('./stream-interface')

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
 * Return stream interfaces. Seee ./stream-interface.
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
function SignalElasticReindexStream () {
  if ( ! ( this instanceof SignalElasticReindexStream ) ) return new SignalElasticReindexStream()

  return streamInterface(SignalElasticReindex)
}
