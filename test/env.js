var debug = require('debug')('env');
var dotenv = require('dotenv-safe');

module.exports = Env;

/**
 * Env configuration object based on dotenv-safe.
 * Options into this function are passed into dotenv-safe.
 *
 * Env configuration includes 
 *
 * SITE_NAME
 * FIREBASE_NAME
 * FIREBASE_KEY
 * 
 * @param {object} options Defaults to process.env
 * @returns {object}   interface
 *          {Function} interface.asObject Returns environment as object
 *          {Function} interface.asString Returns environment as string
 */
function Env ( options ) {
  if ( ! ( this instanceof Env ) ) return new Env( options );
  if ( !options  ) options = {}

  var defaultOptions = {
    path: './test/.env',
    allowEmptyValues: false,
    sample: './test/.env.example',
  }
  
  try {
    var environment = dotenv.load( Object.assign( defaultOptions, options ) ).required;
  } catch ( error ) {
    // These are expected as process.env variables if there is no `.env` file
    debug( 'loading-from-process.env' )
    var environment = Object.assign( {}, process.env );
    debug( environment )
  }

  var configuration = {
    firebase: {
      firebaseName: environment.FIREBASE_NAME,
      firebaseServiceAccountKey : JSON.parse( environment.FIREBASE_SERVICE_ACCOUNT ),
      siteName    : environment.SITE_NAME,
      siteKey     : environment.SITE_KEY,
    },
    signal: {
      payload: {
        userid: environment.SITE_USER,
        sitename: environment.SITE_NAME,
      },
    },
  }

  debug( configuration.firebase )
  debug( configuration.build )

  return {
    asObject: extendConfiguration,
    asString: asString,
  }

  function extendConfiguration () {
    return Object.assign( {}, configuration )
  }

  function asString () {
    var str = '';
    for ( var key in environment ) {
      str = [ str, key, '=', environment[ key ], ' ' ].join( '' )
    }
    return str;
  }
}
