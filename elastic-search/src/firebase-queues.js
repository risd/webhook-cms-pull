var debug = require( 'debug' )( 'firebase-queues' );
var Queue = require( 'firebase-queue' );

var ElasticSearch = require( './elastic-search-api.js' );
var CreateTask = require( './firebase-tasks.js' );


module.exports = Queues;

function Queues ( app ) {

  var elastic = ElasticSearch()
  var create = CreateTask(app)

  var ref = app.database().ref( 'queue' )

  function timeout () {
    return 1000;
  }

  var addQueue = new Queue({
      tasksRef: ref.child( 'search-add-index' ),
      specsRef: ref.child( 'specs' ),
    },
    function ( data, progress, resolve, reject ) {
      // data : { indexName, typeName, document, id, oneOff }
      debug( 'addQueue:data', data )

      elastic.addIndex( data, function ( error, results ) {
        if (error) {
          setTimeout( function () {
            create.addIndex(
              data.indexName,
              data.typeName,
              data.document,
              data.id,
              data.oneOff )
          }, timeout())
          return reject(error);
        }
        else {
          return resolve();
        }
      });
    });

  var deleteQueue = new Queue({
      tasksRef: ref.child('search-delete-index'),
      specsRef: ref.child('spec'),
    },
    function ( data, progress, resolve, reject ) {
      // data: { indexName, typeName, id }
      debug( 'deleteQueue:data', data )

      elastic.deleteIndex( data, function ( error, results ) {
        if (error)  {
          setTimeout( function () {
            create.deleteIndex(
              data.indexName,
              data.typeName,
              data.id )
          }, timeout())
          return reject(error);
        }
        else {
          return resolve();
        }
      })
    });

  process.on( 'SIGINT', function () {
    var checkExit = watchFor( [ 'addQueue', 'deleteQueue' ] )

    addQueue.shutdown().then( function () {
      debug( 'Finished addQueue shutdown' )
      checkExit()
    } )

    deleteQueue.shutdown().then( function () {
      debug( 'Finished deleteQueue shutdown' )
      checkExit()
    } )

    function watchFor( queues ) {
      var watch_count = queues.length;
      return function checkExit () {
        watch_count -= 1;
        if (watch_count === 0) return process.exit(0)
      }
    }

  });

  return {
    add: addQueue,
    delete: deleteQueue,
  };
}
