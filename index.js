const domain = require('domain');
const pg     = require('pg');
const Pool   = require('pg-pool');

function beginTransaction(transaction, client, callback)
{
  client.query('BEGIN', (err, result) => {
    if (err) {
      client.end();
      return callback(err);
    }
    runInDomain(transaction, client, callback);
  });
}

function runInDomain(transaction, client, callback)
{
  var outerDomain = process.domain;
  var d = domain.create();
  d.on('error', (err) => {
    client.end();
    if (outerDomain) outerDomain.emit('error', err);
    if (!transaction.client) return callback(err);
  })
  d.run(() => {
    transaction.client = client;
    return callback(null, transaction);
  });
}

/*** End class PgTransaction ***/
var instance;
module.exports = function(singleton = true) {
  if (singleton) {
    if (instance) return instance;
    else return (instance = new pgutils());
  } else {
    return new pgutils();
  }
};

function pgutils()
{
  var self = this;

  var config = {
    dbConfig : null,
    pool     : null,
    logger   : null
  };

  self.init                 = init;
  self.end                  = end;
  self.getConfig            = getConfig;
  self.execute              = execute;
  self.select               = select;
  self.selectOne            = selectOne;
  self.getExecutorInfo      = getExecutorInfo;
  self.createTransaction    = createTransaction;
  self.beginTransaction     = beginTransaction_;
  self.runTransaction       = runTransaction;

  function init(dbConfig, logger = console)
  {
    config.dbConfig = dbConfig;
    config.pool     = new Pool(dbConfig);
    config.logger   = logger;
  }

  function end()
  {
    config.pool.end()
  }

  function getConfig()
  {
    return config;
  }

  function handleError(err, query, params, callback)
  {
    config.logger.error(err, query, params);
    return callback(err);
  }

  function execute(query, params, callback)
  {
    if (!query) return callback(null, null);
    config.pool.query(query, params, getExecuteHandler(query, params, callback));
  }

  function getExecuteHandler(query, params, callback)
  {
    return function(err, result) {
      if (err) {
        if ('23505' == err.code) {
          return callback({ duplicate: true });
        } else {
          return handleError(err, query, params, callback);
        }
      }
      if (result.rowCount == 0) {
        return callback({ empty: true });
      }
      return callback(null, result.rows, result.rowCount);
    };
  }

  function select(query, params, callback)
  {
    if (!query) return callback(null, null);
    config.pool.query(query, params, function(err, result) {
      if (err) return handleError(err, query, params, callback);
      else if (result.rows.length == 0) return callback({ empty:true });
      else return callback(null, result.rows);
    })
  }

  function selectOne(query, params, callback)
  {
    select(query, params, function(err, rows) {
      if (err) return callback(err);
      return callback(null, (rows) ? rows[0] : {});
    });
  }

  function getExecutorInfo()
  {
    if (arguments.length == 2) return {
      executor : self,
      params   : arguments[0],
      callback : arguments[1]
    };
    if (arguments.length == 3) return {
      executor : arguments[0],
      params   : arguments[1],
      callback : arguments[2]
    };
    return {};
  }

  /** BEGIN class PgTransaction **/
  function PgTransaction() {
    this.client = null;
    return this;
  }

  PgTransaction.prototype.begin = function(dbConfig, callback) {
    var client = new pg.Client(dbConfig);
    client.connect((err) => {
      if (err) return callback(err);
      else beginTransaction(this, client, callback);
    })
  }

  PgTransaction.prototype.execute = function(query, params, callback) {
    if (!this.client) return callback({ message: 'Transaction terminated' });
    this.client.query(query, params, getExecuteHandler(query, params, callback));
  }

  PgTransaction.prototype.commit = function(callback) {
    if (!this.client) return callback({ message: 'Transaction terminated' });
    this.client.query('COMMIT', (err, result) => {
      this.endClient(callback);
    });
  }

  PgTransaction.prototype.rollback = function(callback) {
    if (!this.client) return callback({ message: 'Transaction terminated' });
    this.client.query('ROLLBACK', (err, result) => {
      this.endClient(callback);
    })
  }

  PgTransaction.prototype.endClient = function(callback) {
    this.client.end();
    this.client = null;
    return callback(err);
  }

  PgTransaction.prototype.end = function(err, callback) {
    if (err) {
      this.rollback(function() {
        if (callback) return callback(err);
      });
    } else {
      this.commit(function(err) {
        if (callback) return callback(err);
      });
    }
  }

  function createTransaction()
  {
    return new PgTransaction();
  }

  function beginTransaction_(callback)
  {
    var instance = new PgTransaction();
    instance.begin(config.dbConfig, callback);
    return instance;
  }

  function runTransaction(next, callback)
  {
    return beginTransaction_(function(err, transaction) {
      if (err) return callback(err);
      next(function(err) {
        var prevArguments = arguments;
        transaction.end(err, function(e) {
          if (e) return callback(err);
          return callback(...prevArguments);
        });
      }, transaction);
    });
  }
}