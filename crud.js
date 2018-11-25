const PgUtils  = require('./index')(true);
const NCommons = require('n-commons');
const Logger   = require('n-commons/logger');

const DELETE_AT_CONDITION = 'deleted_at IS NULL';

class Crud
{
  /**
   * markParams: array of array(0: field, 1: value, 2: operator?)
   */
  constructor(tableName, metadata, markParams = null, options = null) {
    this.tableName   = tableName;
    this.metadata    = metadata;
    this.tableFields = Object.keys(metadata);
    this.markParams  = (markParams) ? markParams : [];

    if (!options) {
      this.options = { idField: 'id', useReturning: true, useTimestamp: false };
    } else {
      this.options = options;
      if (undefined === this.options.idField) {
        this.options.idField = 'id';
      }
      if (undefined === this.options.useReturning) {
        this.options.useReturning = true;
      }
      if (undefined === this.options.useTimestamp) {
        this.options.useTimestamp = false;
      }
    }
  }

  create() {
    var exec    = PgUtils.getExecutorInfo(...arguments);
    var dataRaw = [];
    if (this.options.useTimestamp) {
      dataRaw.push(['created_at', 'now()']);
    }
    var query = PgUtils.getInsertSqlBindings(this.tableName, this.tableFields, exec.params, dataRaw);
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
    var query;
    if (id === Object(id)) {
      query = PgUtils.getSelectSqlBindings(this.tableName, this.createSelectBindingOptions(id), id);
    } else {
      query = PgUtils.getSelectSqlBindings(this.tableName, this.createDefaultSelectBindingOptions(id));
    }
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
      whereRaw : this.addDeleteAtCondition()
    };
  }

  addDeleteAtCondition(whereRaw = null) {
    if (this.options.useTimestamp) {
      return (whereRaw) ? whereRaw + ' AND ' + this.getDeleteAtCondition() : this.getDeleteAtCondition();
    } else {
      return whereRaw;
    }
  }

  getDeleteAtCondition() {
    return DELETE_AT_CONDITION;
  }

  createSelectBindingOptions(params) {
    return {
      idField  : this.options.idField,
      fields   : (params.__fields) ? params.__fields : this.tableFields,
      where    : params.__where,
      whereRaw : this.addDeleteAtCondition(params.__whereRaw)
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
    var exec = PgUtils.getExecutorInfo(...arguments);
    var options = this.createUpdateBindingOptions(exec.params);
    if (!this.isUpdateableParams(exec.params)) {
      if (options.useReturning) {
        return this.retrive(exec.params, exec.callback);
      } else {
        return exec.callback(null);
      }
    }
    if (this.options.useTimestamp) {
      if (!options.valuesRaw) {
        options.valuesRaw = [];
      }
      options.valuesRaw.push(['updated_at', 'now()']);
    }
    var query = PgUtils.getUpdateSqlBindings(this.tableName, options, exec.params);
    this.executeQuery(exec, query);
  }

  isUpdateableParams(params) {
    if (params && Object.keys(params).length) {
      for (var i = 0; i < this.tableFields.length; i++) {
        var key = this.tableFields[i];
        if (this.options.idField == key) continue;
        if (typeof params[key] !== 'undefined') return true;
      }
      return false;
    } else {
      return false;
    }
  }

  updateChanges() {
    var exec = PgUtils.getExecutorInfo(...arguments);
    this.retrive(exec.params, NCommons.ok(exec.callback, (row) => {
      var params = crud.getChanges(row, exec.params);
      this.update(exec.executor, params, exec.callback);
    }));
  }

  createUpdateBindingOptions(params) {
    var options = this.createSelectBindingOptions(params);
    options.useReturning = (params.__useReturning) ? params.__useReturning : false;
    options.valuesRaw    = (params.__valueRaw) ? params.__valueRaw : null;
    return options;
  }

  delete() {
    var exec    = PgUtils.getExecutorInfo(...arguments);
    var options = this.createUpdateBindingOptions(exec.params);
    var query   = null;
    if (this.options.useTimestamp && !exec.params.__noTimestamp) {
      if (!options.valuesRaw) {
        options.valuesRaw = [];
      }
      options.valuesRaw.push(['deleted_at', 'now()']);
      query = PgUtils.getUpdateSqlBindings(this.tableName, options, exec.params);
    } else {
      query = PgUtils.getDeleteSqlBindings(this.tableName, options, exec.params);
    }
    this.executeQuery(exec, query);
  }

  mark() {
    var exec    = PgUtils.getExecutorInfo(...arguments);
    var options = this.createUpdateBindingOptions(exec.params);
    if (options.valuesRaw) {
      options.valuesRaw = options.valuesRaw.concat(this.markParams);
    } else {
      options.valuesRaw = this.markParams;
    }
    var query = PgUtils.getUpdateSqlBindings(this.tableName, options, exec.params);
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

  getChanges(oldValues, newValues) {
    var changes = {};
    this.tableFields.forEach((key) => {
      var value = newValues[key];
      if (typeof value !== 'undefined' && (key == this.options.idField || !NCommons.compare(oldValues[key], value))) {
        changes[key] = value;
      }
    });
    return changes;
  }
}

module.exports = Crud;