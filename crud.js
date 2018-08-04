const PgUtils  = require('./index');
const NCommons = require('n-commons');

class Crud
{
  /**
   * markRawParams
     array of array(0: field, 1: value, 2: operator)
   */
  constructor(tableName, tableFields, markRawParams, options = { idField: 'id', useReturning: false }) {
    this.tableName     = tableName;
    this.tableFields   = tableFields;
    this.markRawParams = (markRawParams) ? markRawParams : [];
    this.options       = options;
  }

  create() {
    var exec = PgUtils.getExecutorInfo(...arguments);
    var query = PgUtils.getInsertSqlBindings(this.tableName, this.tableFields, exec.params);
    this.executeQuery(exec, query);
  }

  executeQuery(exec, query) {
    if (query && query.sql) {
      exec.executor.execute(query.sql, query.params, NCommons.ok(exec.callback, function(rows) {
        return exec.callback(null, (rows) ? rows[0] : {});
      }));
    } else {
      return exec.callback(null, {});
    }
  }

  retrive(id, callback) {
    var options = (id === Object(id)) ? this.createSelectBindingOptions(id) : this.createDefaultSelectBindingOptions(id);
    var query = PgUtils.getSelectSqlBindings(this.tableName, options);
    if (query && query.sql) {
      PgUtils.select(query.sql, query.params, NCommons.ok(callback, function(rows) {
        return callback(null, rows[0]);
      }));
    } else {
      return callback(null, {});
    }
  }

  createDefaultSelectBindingOptions(id) {
    return {
      idField      : this.options.idField,
      fields       : [this.options.idField].concat(this.tableFields),
      where        : [[this.options.idField, id]],
    };
  }

  createSelectBindingOptions(params) {
    return {
      idField      : this.options.idField,
      fields       : (params.__fields) ? params.__fields : this.tableFields,
      where        : params.__where,
      whereRaw     : params.__whereRaw
    };
  }

  retriveAll(params, callback) {
    var options = this.createSelectBindingOptions(params);
    var query = PgUtils.getSelectSqlBindings(this.tableName, options);
    if (query && query.sql) {
      PgUtils.select(query.sql, query.params, callback);
    } else {
      return callback(null, []);
    }
  }

  update() {
    var exec    = PgUtils.getExecutorInfo(...arguments);
    var options = this.createUpdateBindingOptions(exec.params);
    var query   = PgUtils.getUpdateSqlBindings(this.tableName, options, exec.params);
    this.executeQuery(exec, query);
  }

  createUpdateBindingOptions(params) {
    var options = this.createSelectBindingOptions(params);
    options.useReturning = (params.__useReturning) ? params.__useReturning : this.options.useReturning;
    return options;
  }

  delete() {
    var exec    = PgUtils.getExecutorInfo(...arguments);
    var options = this.createUpdateBindingOptions(exec.params);
    var query   = PgUtils.getDeleteSqlBindings(this.tableName, options, exec.params);
    this.executeQuery(exec, query);
  }

  markDeleted() {
    var exec    = PgUtils.getExecutorInfo(...arguments);
    var options = this.createUpdateBindingOptions(exec.params);
    options.valuesRaw = this.markRawParams;
    var query   = PgUtils.getUpdateSqlBindings(this.tableName, options, exec.params);
    this.executeQuery(exec, query);
  }
}

module.exports = Crud;