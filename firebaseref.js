var debug = require('debug')('firebaseref');
var from = require('from2-array');
var through = require('through2');

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
 * @param {string} options.firebaseKey   The key of the firebase
 * @param {string} options.siteName      The site instance
 * @param {string} options.siteKey       The key for the site
 * @returns {Stream} firebaseRef
 */

function FirebaseRef ( options, callback ) {
  if ( !options ) options = {}

  return from.obj([ options, null])
             .pipe(FirebaseToken())
             .pipe(FirebaseAuth())
             .pipe(FirebaseBucketForSite())
             .pipe(PushRef( callback ));

  /**
   * @param {object} options
   * @param {object} options.email
   * @param {object} options.password
   * @param {object} options.firebase
   */
  function FirebaseToken () {
      if ( !options ) options = {};
      var request = require('request');
      var authUrl = 'https://auth.firebase.com/auth/firebase';


      return through.obj(createToken);

      function createToken (row, enc, next) {
          var self = this;
          var qs = options;

          debug('token:request');

          request(
              authUrl,
              { qs: options },
              function (err, res, body) {
                  var data = JSON.parse(body);
                  debug('token:reseponse:', JSON.stringify(data));
                  self.push( data );
                  next();
              });
      }
  }

  /**
   * @param {object} options
   * @param {object} options.firebase
   */
  function FirebaseAuth () {
      if ( !options ) options = {}

      var Firebase = require('firebase');
      var dbName = options.firebaseName;
      var dbKey = options.firebaseKey;

      return through.obj(auth);

      function auth (row, enc, next) {
          var self = this;
          var firebase = new Firebase(
                              'https://' +
                              dbName +
                              '.firebaseio.com/');
          debug('auth:token', dbKey);
          firebase
              .auth(
                  dbKey,
                  function (error, auth) {
                      if (error) {
                          console.log(error);
                      } else {
                          self.push({
                              firebaseRoot: firebase
                          });
                      }
                      next();
                  });
      }
  }

  /**
   * @param {object} options
   * @param {string} options.siteName  The site instance
   * @param {string} options.siteKey   The site key
   */
  function FirebaseBucketForSite () {
      var fs = require('fs');
      return through.obj(conf);

      function conf (row, enc, next) {
          row.firebase =
                  row.firebaseRoot
                     .child(
                          'buckets/' +
                          options.siteName +
                          '/' +
                          options.siteKey +
                          '/dev');

          this.push(row);
          next();
      }
  }

  function PushRef ( callback ) {
      var firebaseRef;
      return through.obj(ref, end);

      function ref (row, enc, next) {
          if ( typeof callback !== 'function' ) {
            this.push(row.firebase);
          }
          else {
            firebaseRef = row.firebase;
          }
          next();
      }

      function end () {
        if ( typeof callback === 'function' ) {
          callback( null, firebaseRef )
        }
      }
  }

}
