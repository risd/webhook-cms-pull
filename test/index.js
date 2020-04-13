#!/usr/bin/env node

var debug = require('debug')('sync-courses:cli');
var Sync = require( '../sync.js' );
var Courses = require('./sources/courses/index.js');
var Firebaseref = require('../firebaseref.js');
var path = require( 'path' )

var Env = require('./env.js')();
var envConf = Env.asObject();

var test = require( 'tape' )

var firebase;

// set data to known start state
test( 'set initial data', function ( t ) {
  t.plan( 3 )

  Firebaseref( envConf.firebase, function ( error, fbref ) {
    t.assert( error === null, 'Acquired firebase reference without error' )

    firebase = fbref;

    var initialData = require( path.join( __dirname, 'data', '00-initial-data.json' ) )

    t.assert( typeof initialData === 'object', 'Initial data is an object.' )

    firebase.child( 'data' ).set( initialData )
      .then( function () {
        t.ok( 'initial data was set' )
      } )
  } )
} )

function syncCoursesTest ( fsSource ) {
  return function ( t ) {
    t.plan( 1 )

    envConf.fsSource = fsSource

    Sync( {
      sourcePrototype: Courses,
      env: envConf,
      syncNode: 'syncCourses'
    }, onComplete )

    function onComplete ( error ) {
      t.assert( error === undefined, 'Sync courses finished without error.' )
    }
  }
}

// sync first time
test( 'first sync-courses', syncCoursesTest( [ path.join( __dirname, 'data', '01-courses-to-sync.xml' ) ] ) )


// get data to inspect
test( 'inspect data after first sync', function ( t ) {
  t.plan( 1 )

  firebase.child( 'data' ).once( 'value', function ( dataSnapshot ) {
    var data = dataSnapshot.val()

    t.assert(
      data.employees[ '--employee-key' ]
      .courses_related_employees.length === 1,
      'Employee has a single course'
    )
  } )
} )

// sync second time
test( 'second sync-courses', syncCoursesTest( [ path.join( __dirname, 'data', '02-courses-to-sync.xml' ) ] ) )


// get data to inspect
test( 'inspect data after second sync', function ( t ) {
  t.plan( 1 )

  firebase.child( 'data' ).once( 'value', function ( dataSnapshot ) {
    var data = dataSnapshot.val()

    t.assert(
      data.employees[ '--employee-key' ]
      .courses_related_employees.length === 0,
      'Employee has no courses course'
    )
  } )
} )

test.onFinish( process.exit )
