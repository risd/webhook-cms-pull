var debug = require( 'debug' )( 'create-tasks' )

module.exports = CreateTasks;

function CreateTasks ( app ) {
  if ( ! ( this instanceof CreateTasks ) )
    return new CreateTasks ( app )

  var ref = app.database().ref( 'queue' )

  return {
    addIndex: function ( indexName, typeName, document, id, oneOff ) {
      if ( indexName && typeName && document && id &&
           typeof indexName === 'string' &&
           typeof typeName === 'string' &&
           typeof document === 'string' &&
           typeof id === 'string' ) {

        ref.child( 'search-add-index' ).push( {
          indexName: indexName,
          typeName: typeName,
          document: document,
          id: id,
          oneOff: oneOff,
        } )

        return true
      }

      return false
    },

    deleteIndex: function ( indexName, typeName, id ) {
      if ( indexName && typeName && id &&
           typeof indexName === 'string' &&
           typeof typeName === 'string' &&
           typeof id === 'string' ) {

        ref.child( 'search-delete-index' ).push( {
          indexName: indexName,
          typeName: typeName,
          id: id,
        } )

        return true
      }

      return false
    }
  }
}