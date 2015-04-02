/**
 * @summary Namespace for SQL-related items
 * @namespace
 */
SQL = {};

/**
 * @summary Constructor for a Collection
 * @locus Anywhere
 * @instancename collection
 * @class
 * @param {String} name The name of the collection.  If null, creates an unmanaged (unsynchronized) local collection.
 * @param {Object} [options]
 // We will have to change this
 * @param {Object} options.connection The server connection that will manage this collection. Uses the default connection if not specified.  Pass the return value of calling [`DDP.connect`](#ddp_connect) to specify a different server. Pass `null` to specify no connection. Unmanaged (`name` is null) collections cannot specify a connection.
 * @param {String} options.idGeneration The method of generating the `_id` fields of new documents in this collection.  Possible values:

 - **`'STRING'`**: random strings
 // Confirm with paulo that SQL can generate objectId
 - **`'SQL'`**:  random [`SQL.ObjectID`](#SQL_object_id) values

The default id generation technique is `'STRING'`.
 * @param {Function} options.transform An optional transformation function. Documents will be passed through this function before being returned from `fetch` or `findOne`, and before being passed to callbacks of `observe`, `map`, `forEach`, `allow`, and `deny`. Transforms are *not* applied for the callbacks of `observeChanges` or to cursors returned from publish functions.
 */

SQL.Collection = function(name, options){
  var self = this;
  if (! (self instanceof SQL.Collection))
    throw new Error('use "new" to construct a SQL.Collection');

  if (!name && (name !== null)) {
    Meteor._debug("Warning: creating anonymous collection. It will not be " +
                  "saved or synchronized over the network. (Pass null for " +
                  "the collection name to turn off this warning.)");
    name = null;
  }

  if (name !== null && typeof name !== "string") {
    throw new Error(
      "First argument to new Mongo.Collection must be a string or null");
  }

  if (options && options.methods) {
    // Backwards compatibility hack with original signature (which passed
    // "connection" directly instead of in options. (Connections must have a "methods"
    // method.)
    // XXX remove before 1.0
    options = {connection: options};
  }

    // Backwards compatibility: "connection" used to be called "manager".
  if (options && options.manager && !options.connection) {
    options.connection = options.manager;
  }

  options = _.extend({
    connection: undefined,
    idGeneration: 'STRING',
    transform: null,
    _driver: undefined,
    _preventAutopublish: false
  }, options);

  switch (options.idGeneration) {
  case 'MONGO':
    self._makeNewID = function () {
      var src = name ? DDP.randomStream('/collection/' + name) : Random;
      return new Mongo.ObjectID(src.hexString(24));
    };
    break;
  case 'STRING':
  default:
    self._makeNewID = function () {
      var src = name ? DDP.randomStream('/collection/' + name) : Random;
      return src.id();
    };
    break;
  }

  self._transform = LocalCollection.wrapTransform(options.transform);

  if (! name || options.connection === null)
    // note: nameless collections never have a connection
    self._connection = null;
  else if (options.connection)
    // connections is passed in on the client side
    // it is a large object so we need to see how it is interacted with to better understand it
    self._connection = options.connection;
  else if (Meteor.isClient)
    self._connection = Meteor.connection;
  else
    self._connection = Meteor.server;

  if (!options._driver) {
    // XXX This check assumes that webapp is loaded so that Meteor.server !==
    // null. We should fully support the case of "want to use a Mongo-backed
    // collection from Node code without webapp", but we don't yet.
    // #MeteorServerNul
    if (name && self._connection === Meteor.server &&
        typeof MongoInternals !== "undefined" &&
        MongoInternals.defaultRemoteCollectionDriver) {
      // COME BACK AND UPDATE TO SQL
      options._driver = MongoInternals.defaultRemoteCollectionDriver();
    } else {
      // wont have to change yet
      options._driver = LocalCollectionDriver;
    }
  }

  self._collection = options._driver.open(name, self._connection);
  self._name = name;
  self._driver = options._driver;

  // *******************************************************
  // client side code does not need to be modfied right now.
  // *******************************************************
  if (self._connection && self._connection.registerStore) {
    // OK, we're going to be a slave, replicating some remote
    // database, except possibly with some temporary divergence while
    // we have unacknowledged RPC's.
    var ok = self._connection.registerStore(name, {
      // Called at the beginning of a batch of updates. batchSize is the number
      // of update calls to expect.
      //
      // XXX This interface is pretty janky. reset probably ought to go back to
      // being its own function, and callers shouldn't have to calculate
      // batchSize. The optimization of not calling pause/remove should be
      // delayed until later: the first call to update() should buffer its
      // message, and then we can either directly apply it at endUpdate time if
      // it was the only update, or do pauseObservers/apply/apply at the next
      // update() if there's another one.
      beginUpdate: function (batchSize, reset) {
        // pause observers so users don't see flicker when updating several
        // objects at once (including the post-reconnect reset-and-reapply
        // stage), and so that a re-sorting of a query can take advantage of the
        // full _diffQuery moved calculation instead of applying change one at a
        // time.
        if (batchSize > 1 || reset)
          self._collection.pauseObservers();

        if (reset)
          self._collection.remove({});
      },

      // Apply an update.
      // XXX better specify this interface (not in terms of a wire message)?
      update: function (msg) {
        var mongoId = LocalCollection._idParse(msg.id);
        var doc = self._collection.findOne(mongoId);

        // Is this a "replace the whole doc" message coming from the quiescence
        // of method writes to an object? (Note that 'undefined' is a valid
        // value meaning "remove it".)
        if (msg.msg === 'replace') {
          var replace = msg.replace;
          if (!replace) {
            if (doc)
              self._collection.remove(mongoId);
          } else if (!doc) {
            self._collection.insert(replace);
          } else {
            // XXX check that replace has no $ ops
            self._collection.update(mongoId, replace);
          }
          return;
        } else if (msg.msg === 'added') {
          if (doc) {
            throw new Error("Expected not to find a document already present for an add");
          }
          self._collection.insert(_.extend({_id: mongoId}, msg.fields));
        } else if (msg.msg === 'removed') {
          if (!doc)
            throw new Error("Expected to find a document already present for removed");
          self._collection.remove(mongoId);
        } else if (msg.msg === 'changed') {
          if (!doc)
            throw new Error("Expected to find a document to change");
          if (!_.isEmpty(msg.fields)) {
            var modifier = {};
            _.each(msg.fields, function (value, key) {
              if (value === undefined) {
                if (!modifier.$unset)
                  modifier.$unset = {};
                modifier.$unset[key] = 1;
              } else {
                if (!modifier.$set)
                  modifier.$set = {};
                modifier.$set[key] = value;
              }
            });
            self._collection.update(mongoId, modifier);
          }
        } else {
          throw new Error("I don't know how to deal with this message");
        }

      },

      // Called at the end of a batch of updates.
      endUpdate: function () {
        self._collection.resumeObservers();
      },

      // Called around method stub invocations to capture the original versions
      // of modified documents.
      saveOriginals: function () {
        self._collection.saveOriginals();
      },
      retrieveOriginals: function () {
        return self._collection.retrieveOriginals();
      }
    });

    if (!ok)
      throw new Error("There is already a collection named '" + name + "'");
  }

 /*************************************************************
 // We chose not to include the following line to keep things simple. Can be added later.
 // self._defineMutationMethods();
 *************************************************************/
  /*************************************************************
  // autopublish
  // Someone will have to look into the publishing mechanism and see if this this is needed and how to do it for SQL
  if (Package.autopublish && !options._preventAutopublish && self._connection
      && self._connection.publish) {
    self._connection.publish(null, function () {
      return self.find();
    }, {is_auto: true});
  }
  *************************************************************/
};


///
/// Main collection API
///



_.extend(SQL.Collection.prototype, {

  _getFindSelector: function (args) {
    if (args.length == 0)
      return {};
    else
      return args[0];
  },

  _getFindOptions: function (args) {
    var self = this;
    if (args.length < 2) {
      return { transform: self._transform };
    } else {
      check(args[1], Match.Optional(Match.ObjectIncluding({
        fields: Match.Optional(Match.OneOf(Object, undefined)),
        sort: Match.Optional(Match.OneOf(Object, Array, undefined)),
        limit: Match.Optional(Match.OneOf(Number, undefined)),
        skip: Match.Optional(Match.OneOf(Number, undefined))
     })));

      return _.extend({
        transform: self._transform
      }, args[1]);
    }
  },

    /**
   * @summary Find the documents in a collection that match the selector.
   * @locus Anywhere
   * @method find
   * @memberOf SQL.Collection
   * @instance
   * @param {SQLSelector} [selector] A query describing the documents to find
   * @param {Object} [options]
   * @param {SQLSortSpecifier} options.sort Sort order (default: natural order)
   * @param {Number} options.skip Number of results to skip at the beginning
   * @param {Number} options.limit Maximum number of results to return
   * @param {SQLFieldSpecifier} options.fields Dictionary of fields to return or exclude.
   * @param {Boolean} options.reactive (Client only) Default `true`; pass `false` to disable reactivity
   * @param {Function} options.transform Overrides `transform` on the  [`Collection`](#collections) for this cursor.  Pass `null` to disable transformation.
   * // Probably won't return SQL curso
   * @returns {SQL.Cursor}
   */
  find: function (/* selector, options */) {
    // Collection.find() (return all docs) behaves differently
    // from Collection.find(undefined) (return 0 docs).  so be
    // careful about the length of arguments.
    var self = this;
    var argArray = _.toArray(arguments);
    // PAULO & KATE TO IMPLEMENT
        // return self._collection.find(self._getFindSelector(argArray),
        //                          self._getFindOptions(argArray));
  },

  /**
   * @summary Finds the first document that matches the selector, as ordered by sort and skip options.
   * @locus Anywhere
   * @method findOne
   * @memberOf SQL.Collection
   * @instance
   * @param {SQLSelector} [selector] A query describing the documents to find
   * @param {Object} [options]
   * @param {SQLSortSpecifier} options.sort Sort order (default: natural order)
   * @param {Number} options.skip Number of results to skip at the beginning
   * @param {SQLFieldSpecifier} options.fields Dictionary of fields to return or exclude.
   * @param {Boolean} options.reactive (Client only) Default true; pass false to disable reactivity
   * @param {Function} options.transform Overrides `transform` on the [`Collection`](#collections) for this cursor.  Pass `null` to disable transformation.
   * @returns {Object}
   */
  findOne: function (/* selector, options */) {
    var self = this;
    var argArray = _.toArray(arguments);
    // PAULO AND KATE TO IMPLEMENT
        // return self._collection.findOne(self._getFindSelector(argArray),
        //                             self._getFindOptions(argArray));
  }

});

/*
Eddie and Eric's todos
1) Look into cursors and publishing
2) Look at how the collection mongo.js is called from client and server
*/

