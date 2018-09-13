const PgUtils  = require('./index')(true);
const NCommons = require('n-commons');
const Logger   = require('n-commons/logger');

class Crud
{
  /**
   * markRawParams: array of array(0: field, 1: value, 2: operator?)
   */
  constructor(tableName, tableFields, markRawParams, options = null) {
    this.tableName     = tableName;
    this.tableFields   = tableFields;
    this.markRawParams = (markRawParams) ? markRawParams : [];

    if (!options) {
      this.options = { idField: 'id', useReturning: true };
    } else if (undefined === this.options.useReturning) {
      this.options.useReturning = true;
    }
  }

  create() {
    var exec = PgUtils.getExecutorInfo(...arguments);
    var query = PgUtils.getInsertSqlBindings(this.tableName, this.tableFields, exec.params);
    this.executeQuery(exec, query);
  }

  executeQuery(exec, query) {
    if (query && query.sql) {
      if (Logger.isDebug()) {
        Logger.debug(this.constructor.name + '.executeQuery', query);
      }
      exec.executor.execute(query.sql, query.params, NCommons.ok(exec.callback, function(rows) {
        return exec.callback(null, (rows) ? rows[0] : {});
      }));
    } else {
      return exec.callback(null, {});
    }
  }

  /**
   * id
   * - id value /
   * - { ...field:value...,  __fields, __where, __whereRaw }
   */
  retrive(id, callback) {
    var options = (id === Object(id)) ? this.createSelectBindingOptions(id) : this.createDefaultSelectBindingOptions(id);
    var query = PgUtils.getSelectSqlBindings(this.tableName, options);
    if (query && query.sql) {
      if (Logger.isDebug()) {
        Logger.debug(this.constructor.name + '.retrive', query);
      }
      PgUtils.selectOne(query.sql, query.params, callback);
    } else {
      return callback(null, {});
    }
  }

  createDefaultSelectBindingOptions(id) {
    return {
      idField  : this.options.idField,
      fields   : this.tableFields,
      where    : [[this.options.idField, id]],
    };
  }

  createSelectBindingOptions(params) {
    return {
      idField  : this.options.idField,
      fields   : (params.__fields) ? params.__fields : this.tableFields,
      where    : params.__where,
      whereRaw : params.__whereRaw
    };
  }

  retriveAll(params, callback) {
    var options = this.createSelectBindingOptions(params);
    var query = PgUtils.getSelectSqlBindings(this.tableName, options);
    if (query && query.sql) {
      if (Logger.isDebug()) {
        Logger.debug(this.constructor.name + '.retriveAll', query);
      }
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
    options.useReturning = (params.__useReturning) ? params.__useReturning : false;
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

  getRelations() {
    return null;
  }

  getField(field, alias = '') {
    if (!field || this.tableFields.indexOf(field) == -1) return [];
    if (!alias) return field;
    if ('*' == field) {
      return this.tableFields.map((item) => this.getFieldAs(item, alias));
    } else {
      return this.getFieldAs(field, alias);
    }
  }

  getFieldAs(field, alias) {
    return alias + '.' + field + ' AS ' + alias + '__' + field;
  }
}

module.exports = Crud;