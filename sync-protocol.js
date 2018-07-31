/* Pass in a model, and modify it to get the common 
   functions necessary for sync, and consistent
   across any api/implementation */

var debug = require('debug')('sync-protocol');
var clone = require('clone');
var extend = require('xtend');
var from = require('from2-array');
var through = require('through2');
var combine = require('stream-combiner2');

module.exports = SyncProtocol;

function SyncProtocol (syncRoot, model, firebaseref) {

    var m = ['Model does not conform to Sync protocol.'];


    model.prototype.listFirebaseWebhook = listFirebaseWebhook;
    model.prototype.listFirebaseSource = listFirebaseSource;
    model.prototype.addSourceToWebhook = addSourceToWebhook;
    model.prototype.addInSourceBool = addInSourceBool;
    model.prototype.addToSearchIndex = addToSearchIndex;
    model.prototype.removeFromSearchIndex = removeFromSearchIndex;

    // Resolve relationship pipeline - Start
    model.prototype.rrListWebhookWithRelationshipsToResolve =
        rrListWebhookWithRelationshipsToResolve;
    model.prototype.rrGetRelatedData = rrGetRelatedData;
    model.prototype.rrResetRelated = rrResetRelated;
    model.prototype.rrEnsureReverseRootNode = rrEnsureReverseRootNode;
    model.prototype.rrEnsureReverseContenTypeNode = rrEnsureReverseContenTypeNode;
    model.prototype.rrEnsureReverseContentTypeValueNode = rrEnsureReverseContentTypeValueNode;
    model.prototype.rrEnsureReverseKeyNode = rrEnsureReverseKeyNode;
    model.prototype.rrPopulateRelated = rrPopulateRelated;
    model.prototype.rrSaveReverse = rrSaveReverse;
    model.prototype.rrSaveCurrent = rrSaveCurrent;

    if (typeof model.prototype.relationshipsToResolve !== 'function') {
        m.push('Requires relationshipsToResolve method.');
    }
    if (typeof model.prototype.dataForRelationshipsToResolve !== 'function') {
        m.push('Requires dataForRelationshipsToResolve method.');
    }
    // Resolve relationship pipeline - End


    // Resolve reverse relationship pipeline - Start
    // This pipeline also refers to `relationshipsToResolve`
    // but is accounted for as part of the resolve relationship
    // pipeline, so its not checked for here.
    
    model.prototype.rrrListRelationshipsToResolve = rrrListRelationshipsToResolve;
    model.prototype.rrrAddData = rrrAddData;
    model.prototype.rrrFormatData = rrrFormatData;
    model.prototype.rrrSave = rrrSave;
    
    // Resolve reverse relationship pipeline - End


    // Defaults for overwrittable methods - Start
    if (typeof model.prototype.sourceStreamToFirebaseSource === 'undefined') {
        model.prototype.sourceStreamToFirebaseSource = sourceStreamToFirebaseSource;
    }
    if (typeof model.prototype.updateWebhookValueNotInSource === 'undefined') {
        model.prototype.updateWebhookValueNotInSource = updateWebhookValueNotInSource;
    }
    // Defaults for overwrittable methods - End */
    

    if (typeof model.prototype.webhookContentType !== 'string') {
        m.push('Requires webhookContentType string.');
    }
    if (typeof model.prototype.keyFromSource !== 'function') {
        m.push('Requires keyFromSource method.');
    }
    if (typeof model.prototype.keyFromWebhook !== 'function') {
        m.push('Requires keyFromWebhook method.');
    }
    if (typeof model.prototype.updateWebhookValueWithSourceValue !== 'function') {
        m.push('Requires updateWebhookValueWithSourceValue method.');
    }
    if (typeof model.prototype.listSource !== 'function') {
        m.push('Requires getAllFromSource method.');
    }
    
    if (m.length !== 1) {
        throw new Error(m.join('\n'));
    }

    var type = model.prototype.webhookContentType;
    var webhookDataRoot = 'data';
    var webhookPath = [webhookDataRoot, type].join('/');
    var sourcePath = [syncRoot, type].join('/');
    var reversePath = [syncRoot, 'reverseRelationships'].join('/');

    model.prototype._firebase = {
        webhook: firebaseref.child(webhookPath),
        source:  firebaseref.child(sourcePath),
        reverse: firebaseref.child(reversePath),
        webhookDataRoot: firebaseref.child(webhookDataRoot),
        syncRoot: firebaseref.child(syncRoot),
    };
}

function listFirebaseWebhook () {
    var self = this;

    var eventStream = through.obj();

    self._firebase
        .webhook
        .once('value', onData, onError);

    return eventStream;

    function onData (snapshot) {
        var values = snapshot.val();
        if (values) {
            Object
                .keys(values)
                .forEach(function (key) {
                    eventStream.push({
                        webhook: values[key],
                        whKey: key
                    });
                });
        }
        eventStream.push(null);
    }

    function onError (error) {
        stream.emit('error', error);
    }
}

function listFirebaseSource () {
    var self = this;

    var eventStream = through.obj();

    self._firebase
        .source
        .once('value', onData, onError);

    return eventStream;

    function onData (snapshot) {
        var values = snapshot.val();
        if (values) {
            Object
                .keys(values)
                .forEach(function (key) {
                    eventStream.push({
                        source: values[key],
                        srcKey: self.keyFromSource(values[key])
                    });
                });
        }
        eventStream.push(null);
    }

    function onError (error) {
        debug('list-source-firebase:error')
        debug( error )
        stream.emit('error', error);
    }
}


/**
 * `addSourceToWebhook` is a transform stream.
 * It expects a row of the source key and data.
 * `row.{srcKey, source}`.
 *
 * A snapshot of the current webhook data is
 * captured once to compare against the incoming
 * source data against.
 *
 * In `findKeyInWhData`, the key of every `webhook`
 * entry is compared to the `source` entry that
 * was originally passed through the stream. When
 * a match is found, the `webhook` data is added
 * to the `row`. Coming out of this function will
 * be `row.{srcKey, source, whKey, webhook}`.
 *
 * In `updateWebhook`, the source value,
 * `row.source`, is used to update the webhook
 * value, `row.webhook`. This is a done using
 * the `updateWebhookValueWithSourceValue`
 * defined on the source model prototype. The
 * updated `row.webhook` value is then saved
 * to the key defined by `row.whKey`, if one
 * was found, or a new key is made using the
 * firebase `push` method.
 *
 * This stream pushes the updated `webhook`
 * as part of the `row`.
 * `row.{srcKey, source, whKey, webhook}`
 *
 * @return through.obj stream
 */
function addSourceToWebhook () {
    var self = this;
    var whData = false;

    return combine.obj(
        through.obj(findWhKey),
        through.obj(updateWebhook));

    function findWhKey (row, enc, next) {
        var stream = this;

        if (whData === false) {
            self._firebase
                .webhook
                .once('value', onData, onError);
        } else {
            findKeyInWhData(row, next);
        }

        function onData (snapshot) {
            whData = snapshot.val();
            if (whData === null) {
                whData = {};
            }
            findKeyInWhData(row, next);
        }

        function onError (error) {
            debug( 'could-not-download-data-cache' )
            next(new Error(error));
        }
    }

    function findKeyInWhData (row, callback) {
        row.webhook = {};
        row.whKey = undefined;

        var whEmitter = through.obj();
        var whComparer = through.obj(compareEachItem, onCompareEnd);

        whEmitter.pipe(whComparer);

        Object.keys(whData).forEach(function (key) {
            var whItem = { data: whData[key], key: key };
            whEmitter.push(whItem);
        });
        whEmitter.push(null);

        function compareEachItem (whItem, enc, next) {
            if (self.keyFromWebhook(whItem.data) === row.srcKey) {
                row.webhook = whItem.data;
                row.whKey = whItem.key;
                return next();
            }
            // else if (typeof self.secondaryKeyComparison === 'function') {
            //     var secondaryComparison = extend({}, row, { webhook: whItem.data, whKey: whItem.key });
            //     self.secondaryKeyComparison(secondaryComparison, compareResult)
            //     function compareResult (error, secondaryKeyMatch) {
            //         if (secondaryKeyMatch) {
            //             row.webhook = whItem.data;
            //             row.whKey = whItem.key;
            //         }
            //         return next();
            //     }
            // }
            else {
                return next();
            }
        }

        function onCompareEnd () {
            callback(null, row);
        }
    }

    function updateWebhook (row, enc, next) {
        var stream = this;

        var ref;
        if (row.whKey) {
            ref = self._firebase
                      .webhook
                      .child(row.whKey);
        } else {
            ref = self._firebase
                      .webhook
                      .push();
        }

        var value =
            self.updateWebhookValueWithSourceValue(
                row.webhook,
                row.source);

        ref.set(value, onComplete);

        function onComplete (error) {
            if (error) {
                stream.emit('error', error);
            }
            row.whKey = ref.key();
            row.webhook = value;
            next(null, row);
        }
    }
}

function sourceStreamToFirebaseSource () {
    var self = this;
    return through.obj(toFirebase);

    function toFirebase (row, enc, next) {
        var stream = this;

        var key = self.keyFromSource(row);

        debug('sourceStreamToFirebaseSource:', key);
        
        self._firebase
            .source
            .child(key)
            .set(row, onComplete);

        function onComplete () {
            next();
        }
    }
}


/**
 * `addInSourceBool` is a transform stream.
 * Expects `row.{whKey, webhook}`. A local
 * copy of the `self._firebase.source` will
 * be stashed in a local `sourceData` variable.
 * Each of the `webhook` values will be
 * compared to the `sourceData` values.
 * Incoming `webhook` values that are not
 * in the `sourceData` array will be flagged
 * using `row.inSource`. This will be a boolean
 * value. `true` for in source, `false for not.
 *
 * This stream will push `row` like this:
 * `row.{whKey, webhook, inSource}`
 * 
 * @return through.obj stream
 */
function addInSourceBool () {
    var self = this;
    var srcData = false;
    
    return through.obj(adder);

    function adder (row, enc, next) {
        var stream = this;

        if (srcData === false) {
            self._firebase
                .source
                .once('value', onData, onError);
        } else {
            findKeyInSrcDataAndMark();
        }

        function onData (snapshot) {
            srcData = snapshot.val();
            if (srcData === null) {
                srcData = {};
            }
            findKeyInSrcDataAndMark();
        }

        function onError (error) {
            next(error);
        }

        function findKeyInSrcDataAndMark () {
            row.inSource = false;
            Object
                .keys(srcData)
                .forEach(function (srcKey) {
                    if (self.keyFromSource(srcData[srcKey]) ===
                        self.keyFromWebhook(row.webhook)) {

                        row.inSource = true;
                    }
                });
            next(null, row);
        }
    }
}

/**
 * `updateWebhookValueNotInSource` default method
 * is to remove any `webhook` value that is not
 * represented as a `source` value.
 *
 * Expects `row.inSource` a boolean value.
 * If false, the `webhook` value is not represented
 * in the source values.
 *
 * This is default, which removes the entry.
 * This can be overwritten per model.
 * 
 * @return through.obj stream
 */
function updateWebhookValueNotInSource () {
    var self = this;
    return through.obj(updateNotInSource);

    function updateNotInSource (row, enc, next) {
        var stream = this;
        if (row.inSource === false) {
            // debug('Not in source. Remove.');
            self._firebase
                .webhook
                .child(row.whKey)
                .remove(function onComplete () {
                    next(null, row);
                });
        } else {
            next();
        }
    }
}


/**
 * `addToSearchIndex` runs after `addSourceToWebHook`
 * with the current row = { whKey, srcKey, webhook, source }
 * 
 * @param {function} searchAddIndex Function to add to
 *                                  search index
 * @return through.obj stream
 */
function addToSearchIndex (searchAddIndex) {
    var self = this;

    return through.obj(addToSearch);

    function addToSearch (row, enc, next) {
        var typeName = self.webhookContentType
        var document = row.webhook
        var id       = row.whKey
        var oneOff   = false

        searchAddIndex(typeName, document, id, oneOff, function () {
            next(null, row);
        });
    }
}

/**
 * `removeFromSearchIndex` occurs after
 * `updateWebhookValueNotInSource`, and only
 * operates on values that trickle through,
 * values not in the source
 *
 * 
 * @param {function} searchDeleteIndex Function to delete
 *                                     from search index
 * @return through.obj stream
 */
function removeFromSearchIndex (searchDeleteIndex) {
    var self = this;
    return through.obj(removeFromSearch);

    function removeFromSearch (row, enc, next) {
        var typeName = self.webhookContentType;
        var id       = row.whKey;

        searchDeleteIndex(typeName, id, function () {
            next(null, row);
        });
    }
}


/* relationship resolution - rr */

/**
 * push row.{webhook, whKey, toResolve}
 * for every relationship that needs to get
 * resolved. if there are two relationships,
 * two objects get pushed before saving,
 * merge the items back together.
 * 
 * @return {stream} through.obj
 */
function rrListWebhookWithRelationshipsToResolve () {
    debug( 'init-list-to-resolve' )
    var self = this;

    var eventStream = through.obj();

    self._firebase
        .webhook
        .once('value', onData, onError);

    return eventStream;

    function onData (snapshot) {
        var values = snapshot.val();
        if (values) {
            var rows = Object
                .keys(values)
                .map(function (key) {
                    return {
                        webhook: values[key],
                        whKey: key
                    };
                })
                .map(function (row) {
                    self.dataForRelationshipsToResolve(
                            row.webhook)
                        .map(function (toResolve) {
                            row.toResolve = toResolve;
                            var toPush = clone(row);
                            eventStream.push(toPush);
                        });

                });
        }
        eventStream.push(null);
    }

    function onError (error) {
        debug( 'list-to-resolve:error' )
        debug( error )
        eventStream.emit('error', error);
    }
}

// if the relationship to resolve can have
// multiple related values, stash those values
// in `relatedDataCollection`. Otherwise its a
// relationship to a one-off content type, in
// which case stash the data in relatedDataItem
function rrGetRelatedData () {
    var self = this;

    // cache data that is being used to resolve
    // the relationships. this data does not get
    // changed, only id's get referenced for
    // creating the relationships, then stashed
    // if they are used, to create the reverse
    // relationships
    var cache = {};

    return through.obj(get);

    function get (row, enc, next) {
        debug('Get related data');
        row.relatedDataCollection = false;
        row.relatedDataItem = false;
        var stream = this;

        debug('\n\nTo resolve');
        // debug(row.whKey);
        debug(row.toResolve.relationshipKey);
        

        if ((row.toResolve.multipleToRelate === true) &&
            (row.toResolve.itemsToRelate.length === 0)) {
            this.push(row);
            next();
        }
        else if ((row.toResolve.multipleToRelate === false) &&
                 (row.toResolve.itemToRelate === false)) {
            this.push(row);
            next();
        }
        else {
            if (Object
                    .keys(cache)
                    .indexOf(row.toResolve
                                .relateToContentType) === -1) {
                // Data is not cached
                self._firebase
                    .webhookDataRoot
                    .child(row.toResolve.relateToContentType)
                    .once('value', function (snapshot) {
                        var value = snapshot.val();
                        cache[row.toResolve
                                 .relateToContentType] = value;

                        if (row.toResolve.multipleToRelate) {
                            row.relatedDataCollection = value;    
                        }
                        else {
                            row.relatedDataItem = value;
                        }
                        stream.push(row);
                        next();
                    });
            } else {
                if (row.toResolve.multipleToRelate) {
                    row.relatedDataCollection =
                        clone(cache[row.toResolve
                                   .relateToContentType]);
                }
                else {
                    row.relatedDataItem =
                        clone(cache[row.toResolve
                                   .relateToContentType]);
                }
                this.push(row);
                next();
            }
        }
    }
}

function rrResetRelated () {
    var self = this;

    return through.obj(reset);

    function reset (row, enc, next) {

        row.webhook
           [row.toResolve.relationshipKey] = [];

        this.push(row);
        next();
    }
}

function rrEnsureReverseRootNode () {
    // does not modify `row`, only
    // makes sure there an object
    // for the sync to write to
    // This only needs to be run once,
    // as part of the initial setup
    // for resolving relationships.
    // 
    // eduSync/reverseRelationships
    var self = this;
    var exists = false;

    return through.obj(ensure);

    function ensure (row, enc, next) {
        var stream = this;
        if (exists) {
            done();
        } else {
            self._firebase
                .reverse
                .set({}, function () {
                    exists = true;
                    done();
                });
        }

        function done () {
            stream.push(row);
            next();
        }
    }
}

function rrEnsureReverseContenTypeNode () {
    // does not modify `row`. only
    // makes sure there is an object
    // for reverse values to be written to
    // 
    // can be executed multiple times, since
    // the content type node is going to be
    // determined by the row coming through
    // 
    // eduSync/reverseRelationships/{contentType}
    var self = this;

    return through.obj(ensure);

    function ensure (row, enc, next) {
        var stream = this;

        if (row.relatedDataCollection) {
            var keyToEnsure = row.toResolve.relateToContentType;
            self._firebase
                .reverse
                .once('value', function (snapshot) {
                    if (snapshot.hasChild(keyToEnsure)) {
                        done();
                    } else {
                        makeKey(keyToEnsure);
                    }
                });
        } else {
            done();
        }

        function makeKey (key) {
            self._firebase
                .reverse
                .child(key)
                .set({}, function () {
                    done();
                });
        }

        function done () {
            stream.push(row);
            next();
        }
    }
}

function rrEnsureReverseContentTypeValueNode () {
    // does not modify `row`. only
    // makes sure there is an object
    // for reverse values to be written to
    // 
    // can be executed multiple times, since
    // the content type value node is going to be
    // determined by the row coming through
    // 
    // Relationship to content type with
    // multiple values:
    // eduSync/
    // reverseRelationships/
    // {contentType}/
    // {contentTypeKey}/
    var self = this;

    return through.obj(ensure);

    function ensure (row, enc, next) {
        var stream = this;

        if (row.relatedDataCollection) {
            var keyToEnsure = row.whKey;
            self._firebase
                .reverse
                .child(row.toResolve.relateToContentType)
                .once('value', function (snapshot) {
                    if (snapshot.hasChild(keyToEnsure)) {
                        done();
                    } else {
                        makeKey(keyToEnsure);
                    }
                });
        }
        else {
            done();
        }

        function makeKey (key) {
            self._firebase
                .reverse
                .child(row.toResolve.relateToContentType)
                .child(key)
                .set({}, function () {
                    done();
                });
        }

        function done () {
            stream.push(row);
            next();
        }
    }
}

function rrEnsureReverseKeyNode () {
    // does not modify `row`, only
    // makes sure there is an object
    // for reverse values to be written to
    // 
    // can be executed multiple times, since
    // the content type node is going to be
    // determined by the row coming through
    // 
    // Relationship to content-type with
    // multiple values.
    // eduSync/
    //   reverseRelationships/
    //   {contentType}/
    //   {contentTypeKey}/
    //   {reverseKey}
    //   
    // Relationship with one-off content-type
    // eduSync/
    //   reverseRelationships/
    //   {contentType}/
    //   {reverseKey}/
    var self = this;
    return through.obj(ensure);

    function ensure (row, enc, next) {
        var stream = this;

        var keyToEnsure =
                [self.webhookContentType,
                 row.toResolve.relationshipKey].join('_');
        var ref;

        if (row.relatedDataCollection || row.relatedDataItem) {
            if (row.relatedDataCollection) {
                ref = self._firebase
                    .reverse
                    .child(row.toResolve.relateToContentType)
                    .child(row.whKey);
            }
            else if (row.relatedDataItem) {
                ref = self._firebase
                    .reverse
                    .child(row.toResolve.relateToContentType);
            }

            ref.once('value', function (snapshot) {
                    if (snapshot.hasChild(keyToEnsure)) {
                        done();
                    } else {
                        makeKey(ref, keyToEnsure);
                    }
                });
        }
        else {
            done();
        }

        function makeKey (ref, key) {
            ref.child(key)
                .set({}, function () {
                    done();
                });
        }

        function done () {
            stream.push(row);
            next();
        }
    }
}

function rrPopulateRelated () {
    var self = this;

    // If something is getting updated, it will
    // likely occur here.
    return through.obj(populate);

    function populate (row, enc, next) {
        debug('rrPopulateRelated');
        // debug(row.toResolve.relationshipKey);

        row.reverseToSave = {};

        if (row.relatedDataCollection) {
            Object
                .keys(row.relatedDataCollection)
                .forEach(function (relatedKey) {
                    row.reverseToSave[relatedKey] = {};

                    var related =
                        row.relatedDataCollection
                           [relatedKey]
                           [row.toResolve
                               .relateToContentTypeDataUsingKey];

                    // debug(related);
                    row.toResolve
                        .itemsToRelate
                        .forEach(function (itemToRelate) {
                            var relate =
                                itemToRelate
                                    [row.toResolve
                                        .relateToContentType];

                            var reverseKey = [
                                    self.webhookContentType,
                                    row.toResolve.relationshipKey
                                ].join('_');

                            if (Object
                                    .keys(row.reverseToSave[relatedKey])
                                    .indexOf(reverseKey) === -1) {

                                row.reverseToSave
                                   [relatedKey]
                                   [reverseKey] = {};
                            }

                            // debug(relate);
                            if (related === relate) {
                                // debug('Match!');
                                // sort out updating objects

                                var relationshipValue = [
                                        row.toResolve.relateToContentType,
                                        relatedKey
                                    ].join(' ');

                                if (row.webhook
                                        [row.toResolve.relationshipKey]
                                            .indexOf(relationshipValue) === -1) {

                                    row.webhook
                                        [row.toResolve.relationshipKey]
                                                .push(relationshipValue);
                                }

                                var reverseValue = [
                                        self.webhookContentType,
                                        row.whKey
                                    ].join(' ');

                                row.reverseToSave
                                    [relatedKey]
                                    [reverseKey]
                                    [reverseValue] = true;
                            }
                        });
                });
        }
        else if(row.relatedDataItem) {

            var reverseKey = [
                    self.webhookContentType,
                    row.toResolve.relationshipKey
                ].join('_');
            row.reverseToSave[reverseKey] = {};

            if (row.toResolve.itemToRelate) {
                var relationshipValue =
                    [row.toResolve.relateToContentType,
                     row.toResolve.relateToContentType].join(' ');

                if (row.webhook
                       [row.toResolve.relationshipKey]
                       .indexOf(relationshipValue) === -1) {

                    row.webhook
                       [row.toResolve.relationshipKey]
                       .push(relationshipValue);
                }

                var reverseValue = [
                        self.webhookContentType,
                        row.whKey
                    ].join(' ');

                row.reverseToSave
                    [reverseKey]
                    [reverseValue] = true;
            }
        }

        this.push(row);
        next();
    }
}

function rrSaveReverse () {
    var self = this;

    return through.obj(save);

    function save (row, enc, next) {
        debug('\n\nrrSaveReverse');
        debug(row.toResolve.relationshipKey);

        var stream = this;

        var toSaveKeys = Object.keys(row.reverseToSave);
        var toSave = [];

        if (toSaveKeys.length === 0) {
            this.push(row);
            next();
        } else {
            if (row.relatedDataCollection) {
                // debug('Save reverse multiple.');
                var relatedKeys = toSaveKeys;
                relatedKeys.forEach(function (relatedKey) {
                        var reverseKeys =
                            Object.keys(
                                    row.reverseToSave[relatedKey]);

                        reverseKeys.forEach(function (reverseKey) {

                            var reverseValues = Object.keys(
                                    row.reverseToSave
                                        [relatedKey]
                                        [reverseKey]);

                            reverseValues.forEach(function (reverseValue) {
                                toSave.push({
                                    contentType:
                                        row.toResolve.relateToContentType,
                                    relatedKey: relatedKey,
                                    reverseKey: reverseKey,
                                    reverseValue: reverseValue
                                });
                            });
                        });
                    });
            }
            else if (row.relatedDataItem)  {
                var reverseKey = toSaveKeys[0];
                debug(row);
                var reverseValue =
                    Object
                        .keys(row.reverseToSave[reverseKey])
                        .pop();

                if (reverseValue) {
                    toSave.push({
                        contentType:
                            row.toResolve.relateToContentType,
                        reverseKey: reverseKey,
                        reverseValue: reverseValue
                    });
                }
            }

            toSaveCount = toSave.length;

            if (toSaveCount === 0) {
                this.push(row);
                next();
            }
            else {
                toSave.map(saver)
                      .map(watcher);
            }
        }


        function saver (d) {
            debug('rrSaveReverse:saver::start');
            debug(d);

            var t = through.obj();
            var ref;

            // relatedKey is only used for
            // content types that have multiple
            // entries
            if ('relatedKey' in d) {
                ref = self._firebase
                          .reverse
                          .child(d.contentType)
                          .child(d.relatedKey)
                          .child(d.reverseKey)
                          .child(d.reverseValue);
            }
            // if no relatedKey, it means we are
            // looking at a one-off content-type
            else {
                ref = self._firebase
                          .reverse
                          .child(d.contentType)
                          .child(d.reverseKey)
                          .child(d.reverseValue);
            }
            
            ref.set(true, function () {
                    debug('rrSaveReverse:saver::end');
                    t.push({});
                    t.push(null);
                });

            return t;
        }

        function watcher (s) {
            s.on('data', function () {});
            s.on('end', function () {
                toSaveCount -= 1;
                if (toSaveCount === 0) {
                    // debug('Save reverse::done');
                    stream.push(row);
                    next();
                }
            });
        }
    }
}

function rrSaveCurrent () {
    var self = this;

    var current = false;
    return through.obj(save);

    function save (row, enc, next) {
        debug('\n\nSave current.');
        debug(row.whKey);
        debug(row.toResolve.relationshipKey);
        // debug(row.webhook[row.toResolve.relationshipKey]);

        var stream = this;

        self._firebase
            .webhook
            .child(row.whKey)
            .child(row.toResolve.relationshipKey)
            .set(row.webhook[row.toResolve.relationshipKey],
                function () {
                    debug('\n\nSave current::end');
                    debug(self.webhookContentType);
                    debug(row.toResolve.relationshipKey);
                    next();
                });
    }
}

/* end relationship resolution - rr */

/* resolve reverse relationship - rrr - start */
function rrrListRelationshipsToResolve () {
    debug('rrrListRelationshipsToResolve');
    var self = this;

    var stream = through.obj();

    self.relationshipsToResolve()
        .forEach(function (toResolve) {
            var row = {};
            row.toResolve = toResolve;
            debug('rrr to Resolve');
            debug(toResolve);
            stream.push(row);
        });
    
    stream.push(null);

    return stream;
}

function rrrAddData () {
    // pushes
    // row.{toResolve, {reverseContentTypeCollection,
    //                  reverseContentTypeItem}
    //     }
    var self = this;

    return through.obj(add);

    function add (row, enc, next) {
        var stream = this;
        row.reverseContentTypeCollection = false;
        row.reverseContentTypeItem = false;

        // debug('\n\nreverse data');
        // debug(row.toResolve.relateToContentType);

        self._firebase
            .reverse
            .child(row.toResolve.relateToContentType)
            .once('value', function (snapshot) {
                var value = snapshot.val();
                if (value) {
                    if (row.toResolve.multipleToRelate) {
                        // debug('reverseContentTypeCollection');
                        // debug(value);
                        row.reverseContentTypeCollection = value;
                    } else {
                        // debug('reverseContentTypeItem');
                        // debug(value);
                        row.reverseContentTypeItem = value;
                    }
                }
                stream.push(row);
                next();
            });
    }
}

function rrrFormatData () {
    // pushes data to update
    // row.{toSave: { key, value }}
    var self = this;

    return through.obj(format);

    function format (row, enc, next) {
        var stream = this;
        var toSave = [];

        if (row.reverseContentTypeCollection) {
            Object
                .keys(row.reverseContentTypeCollection)
                .forEach(function (contentTypeKey) {
                    Object
                        .keys(row.reverseContentTypeCollection[contentTypeKey])
                        .forEach(function (reverseKey) {

                            var reverseValue = [];

                            Object
                                .keys(row.reverseContentTypeCollection[contentTypeKey][reverseKey])
                                .forEach(function (singleReverseValue) {
                                    reverseValue.push(singleReverseValue);
                                });

                            toSave.push({
                                contentType: row.toResolve.relateToContentType,
                                contentTypeKey: contentTypeKey,
                                reverseKey: reverseKey,
                                reverseValue: reverseValue
                            });
                        });
                });
        }
        else if (row.reverseContentTypeItem) {
            // debug('row.reverseContentTypeItem');
            // debug(row.reverseContentTypeItem);
            var reverseKey = Object.keys(row.reverseContentTypeItem)
                                   .pop();

            var reverseValue = [];

            Object
                .keys(row.reverseContentTypeItem
                         [reverseKey])
                .forEach(function (singleReverseValue) {
                    reverseValue.push(singleReverseValue);
                });

            toSave.push({
                contentType:
                    row.toResolve.relateToContentType,
                reverseKey: reverseKey,
                reverseValue: reverseValue
            });
        }

        this.push(toSave);
        next();
    }
}

function rrrSave () {
    var self = this;

    return through.obj(save);

    function save (toSave, enc, next) {
        var stream = this;
        var saverCount = toSave.length;

        if (saverCount > 0) {
            // debug('to save');
            // debug(toSave);
            toSave
                .map(saver)
                .map(watcher);
        } else {
            this.push({});
            next();
        }

        function saver (d) {
            var t = through.obj();
            var ref;
            if ('contentTypeKey' in d) {
                ref = self._firebase
                          .webhookDataRoot
                          .child(d.contentType)
                          .child(d.contentTypeKey)
                          .child(d.reverseKey);
            }
            else {
                ref = self._firebase
                          .webhookDataRoot
                          .child(d.contentType)
                          .child(d.reverseKey);

            }
            
            ref.set(d.reverseValue, function () {
                    t.push({});
                    t.push(null);
                });

            return t;
        }

        function watcher (s) {
            s.on('data', function () {});
            s.on('end', function () {
                saverCount -= 1;
                if (saverCount === 0) {
                    done();
                }
            });
        }

        function done () {
            debug('rrr resolved');
            debug(toSave);
            next();
        }
    }
}
/* resolve reverse relationship - rrr - end */
