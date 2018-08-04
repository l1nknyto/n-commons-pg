const domain = require('domain');
const pg     = require('pg');
const Pool   = require('pg-pool');

var config = {
  dbConfig : null,
  pool     : null,
  logger   : null
};

function init(dbConfig, logger = console)
{
  config.dbConfig = dbConfig;
  config.pool     = new Pool(dbConfig);
  config.logger   = logger;
}

function execute(query, params, callback)
{
  if (!query) return callback(null, null);
  config.pool.query(query, params, function(err, result) {
    if (err) return handleError(err, query, params, callback);
    else if (result.rowCount == 0) return callback({ empty:true });
    else return callback(null, result.rows);
  })
}

function handleError(err, query, params, callback)
{
  config.logger.error(err, query, params);
  return callback(err);
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

function end()
{
  config.pool.end()
}

function getExecutorInfo()
{
  if (arguments.length == 2) return {
    executor : instance,
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

/**
 * options
     fields
     where
     whereRaw
 */
function getSelectSqlBindings(table, options, data)
{
  var sqlParams = {
    params   : [],
    whereSql : ''
  };
  if (options.fields && options.fields.length) {
    var sql = createSelectSql(sqlParams, table, options, data);
    return {
      sql    : sql,
      params : sqlParams.params
    };
  } else {
    return null;
  }
}

function createSelectSql(sqlParams, tableName, options, data)
{
  createWhereCondition(sqlParams, options, data);
  return 'SELECT ' + options.fields.join(', ') + ' FROM '+ tableName + sqlParams.whereSql;;
}

function getInsertSqlBindings(tableName, tableFields, data)
{
  var fields = []; var values = []; var params = [];
  for (var i = 0, j = 1; i < tableFields.length; i++) {
    var key   = tableFields[i];
    var value = data[key];
    if (typeof value !== 'undefined') {
      fields.push(key);
      values.push('$'+ j); j++;
      params.push(value);
    }
  }
  if (values.length) {
    return {
      sql    : "INSERT INTO "+ tableName +"("+ fields.join(',') +") VALUES("+ values.join(',') +") RETURNING *",
      params : params
    };
  } else {
    return null;
  }
}

/**
 * tableFields
 *  as Array of fields
 *  as Object(fields, idField, useReturning, where, whereRaw)
 */
function getUpdateSqlBindings(table, tableFields, data)
{
  var options = (Array.isArray(tableFields)) ? getDefaultOptions(tableFields) : tableFields;
  var sqlParams = {
    params         : [],
    updateValueSql : '',
    whereSql       : ''
  };
  createUpdateFieldValues(sqlParams, options, data);
  if (sqlParams.updateValueSql) {
    var sql = createUpdateSql(sqlParams, table, options, data);
    return {
      sql    : sql,
      params : sqlParams.params
    };
  } else {
    return null;
  }
}

function getDefaultOptions(fields)
{
  return {
    fields       : fields,
    idField      : 'id',
    where        : [],
    whereRaw     : '',
    useReturning : true
  };
}

function createUpdateFieldValues(sqlParams, options, data)
{
  var values = [];

  if (options.fields) {
    for (var i = 0, j = sqlParams.params.length; i < options.fields.length; i++) {
      var key   = options.fields[i];
      var value = data[key];
      if (typeof value !== 'undefined') {
        if (options.idField != key) { //TODO: id can be updated?
          sqlParams.params.push(value);
          values.push(key + '=$' + ++j);
        }
      }
    }
  }

  if (options.valuesRaw) {
    for (var i = 0; i < options.valuesRaw.length; i++) {
      var value    = options.valuesRaw[i];
      var operator = (value.length > 2) ? value[2] : '=';
      values.push(value[0] + operator + String(value[1]));
    }
  }

  if (values.length) {
    sqlParams.updateValueSql = values.join(', ');
  }
  return sqlParams;
}

function createUpdateSql(sqlParams, tableName, options, data)
{
  createWhereCondition(sqlParams, options, data);
  var sql = 'UPDATE ' + tableName +' SET ' + sqlParams.updateValueSql + sqlParams.whereSql;
  if (options.useReturning) {
    sql = sql + ' RETURNING *';
  }
  return sql;
}

function createWhereCondition(sqlParams, options, data)
{
  var whereValues = [];

  if (data && data[options.idField]) {
    sqlParams.params.push(data[options.idField]);
    whereValues.push([options.idField + '=$' + sqlParams.params.length, ' AND ']);
  }

  if (options.where) {
    for (var i = 0, j = sqlParams.params.length; i < options.where.length; i++) {
      var conditions  = options.where[i];
      var operator    = (conditions.length > 2) ? conditions[2] : '=';
      var conjunction = (conditions.length > 3) ? ' ' + conditions[3] + ' ' : ' AND ';
      sqlParams.params.push(conditions[1]);
      whereValues.push([conditions[0] + operator + '$' + ++j, conjunction]);
    }
  }

  var sql = (options.whereRaw) ? options.whereRaw : '';
  for (var i = 0; i < whereValues.length; i++) {
    var conditions  = whereValues[i];
    if (sql) {
      sql = sql + conditions[1] + conditions[0];
    } else {
      sql = conditions[0];
    }
  }

  if (sql) {
    sqlParams.whereSql = ' WHERE ' + sql;
  } else {
    sqlParams.whereSql = '';
  }
  return sqlParams;
}

function getDeleteSqlBindings(table, tableFields, data)
{
  var options = (Array.isArray(tableFields)) ? getDefaultOptions(tableFields) : tableFields;
  var sqlParams = {
    params         : [],
    updateValueSql : '',
    whereSql       : '',
  };
  var sql = createDeleteSql(sqlParams, table, options, data);
  return {
    sql    : sql,
    params : sqlParams.params
  };
}

function createDeleteSql(sqlParams, tableName, options, data)
{
  createWhereCondition(sqlParams, options, data);
  var sql = 'DELETE FROM ' + tableName + sqlParams.whereSql;
  if (options.useReturning) {
    sql = sql + ' RETURNING *';
  }
  return sql;
}

/**
 * BEGIN class PgTransaction
 */
function PgTransaction() {
  this.client = null
  return this
}

PgTransaction.prototype.begin = function(callback) {
  var client = new pg.Client(config.dbConfig)
  client.connect((err) => {
    if (err) return callback(err)
    else beginTransaction(this, client, callback)
  })
}

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

PgTransaction.prototype.execute = function(query, queryParams, callback) {
  if (!this.client) return callback({ message:'No transaction.' });
  this.client.query(query, queryParams, function(err, result) {
    if (err) callback(err);
    else if (result.rowCount == 0) return callback({ empty:true });
    else return callback(null, result.rows);
  });
}

PgTransaction.prototype.commit = function(callback) {
  if (!this.client) return callback(null)
  this.client.query('COMMIT', (err, result) => {
    this.client.end()
    return callback(err)
  })
}

PgTransaction.prototype.rollback = function(callback) {
  if (!this.client) return callback(null)
  this.client.query('ROLLBACK', (err, result) => {
    this.client.end()
    return callback(err)
  })
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
  var config = new PgTransaction();
  config.begin(callback);
  return config;
}

function runTransaction(next, callback)
{
  return beginTransaction_(function(err, transaction) {
    if (err) {
      return callback(err);
    }
    next(function(err) {
      var prevArguments = arguments;
      transaction.end(err, function(e) {
        if (e) {
          return callback(err);
        }
        return callback(...prevArguments);
      });
    }, transaction);
  });
}
/*** End class PgTransaction ***/

var instance = {
  init                 : init,
  end                  : end,
  execute              : execute,
  select               : select,
  selectOne            : selectOne,
  getExecutorInfo      : getExecutorInfo,
  getSelectSqlBindings : getSelectSqlBindings,
  getInsertSqlBindings : getInsertSqlBindings,
  getUpdateSqlBindings : getUpdateSqlBindings,
  getDeleteSqlBindings : getDeleteSqlBindings,
  createTransaction    : createTransaction,
  beginTransaction     : beginTransaction_,
  runTransaction       : runTransaction
};

module.exports = instance;