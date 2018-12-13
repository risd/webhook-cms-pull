var debug = require('debug')('firebaseref');
var admin = require('firebase-admin');
var from = require('from2-array');
var through = require('through2');

var firebaseEscape = require( './util/firebase-escape.js' )

module.exports = FirebaseRef;

/**
 * FirebaseRef
 * 
 * Returns object stream that pushes a
 * firebase object that has been configured
 * for the current WebHook site.
 *
 * @param {object} options
 * @param {string} options.firebaseName  The name of the firebase
 * @param {string} options.firebaseServiceAccountKey   The key of the firebase
 * @param {string} options.siteName      The site instance
 * @param {string} options.siteKey       The key for the site
 * @param {function?} callback           Optional function to pass the firebaseRef to.
 * @returns {Stream} stream              If no callback is passed, a stream is created to push the firebaseRef to.
 */

function FirebaseRef ( options, callback ) {

  var firebaseName = options.firebaseName
  var firebaseServiceAccountKey = options.firebaseServiceAccountKey
  var initializationName = options.initializationName || '[DEFAULT]'
  var siteName = firebaseEscape( options.siteName )
  var siteKey = options.siteKey

  var app = appForName( initializationName )
  if ( ! app ) {
    app = admin.initializeApp( {
      databaseURL: 'https://' + firebaseName + '.firebaseio.com',
      credential: firebaseServiceAccountKey,
    } , initializationName )
  }

  var firebaseRef = app.database()
    .ref( 'buckets' )
    .child( siteName )
    .child( siteKey )
    .child( 'dev' )

  if ( typeof callback === 'function' ) {
    return callback( null, firebaseRef )
  }

  var stream = throuhg.obj()

  process.nextTick( function pushRef () {
    stream.push( firebaseRef )
    stream.push( null )
  } )

  return stream;

  function appForName ( name ) {
    var appOfNameList = admin.apps.filter( appOfName )
    if ( appOfNameList.length === 1 ) return appOfNameList[ 0 ]
    return null

    function appOfName ( app ) {
      return app.name === name
    }
  }
}
