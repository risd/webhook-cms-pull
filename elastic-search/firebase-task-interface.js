var debug = require( 'debug' )( 'elastic-search-sync' )
var CreateTasks = require( './src/firebase-tasks.js' )

// Not currently used

module.exports = ElasticSearchSync;

function ElasticSearchSync ( namespace ) {
  if ( ! ( this instanceof ElasticSearchSync ) ) return new ElasticSearchSync( namespace );

  var namespace = namespace || 'default';

  var firebase = require( 'firebase' );
  // var admin = require( 'firebase-admin' );

  var firebaseProject = process.env.FIREBASE_PROJECT;
  var firebaseServiceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
  var indexName = process.env.ELASTIC_SEARCH_INDEX;

  var app = firebase.initializeApp({
    serviceAccount: firebaseServiceAccount,
    databaseURL: "https://" + firebaseProject + ".firebaseio.com",
  }, namespace);

  // var app = admin.initializeApp({
  //   // credential: admin.credential.cert(firebaseServiceAccount),
  //   credential: {
  //     projectId: firebaseServiceAccount.project_id,
  //     clientEmail: firebaseServiceAccount.client_email,
  //     privateKey: firebaseServiceAccount.private_key,
  //   },
  //   databaseURL: "https://" + firebaseProject + ".firebaseio.com",
  // }, namespace);

  var create = CreateTasks( app )

  function addIndex ( typeName, document, id, oneOff ) {
    return create.addIndex( indexName, typeName, document, id, oneOff );
  }

  function deleteIndex ( typeName, id ) {
    return create.deleteIndex( indexName, typeName, id );
  }

  return {
    addIndex: addIndex,
    deleteIndex: deleteIndex,
  }
}
