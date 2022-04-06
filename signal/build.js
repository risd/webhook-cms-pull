var debug = require('debug')('signal-build');
var signal = require( './index' )
var streamInterface = require('./stream-interface')

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

  return streamInterface(SignalBuild)
}
