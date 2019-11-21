const _ = require('underscore');
const PgUtils = require('./index')();
const NCommons = require('n-commons');
const Logger = require('n-commons/logger');
const SelectBuilder = require('./builders/select-builder');
const InsertBuilder = require('./builders/insert-builder');
const UpdateBuilder = require('./builders/update-builder');
const DeleteBuilder = require('./builders/delete-builder');
const CrudInterface = require('./crud.interface');

class Crud extends CrudInterface {

  /**
   * markParams: array of array(0: field, 1: value, 2: operator?)
   */
  constructor(tableName, metadata, markParams = null, options = null) {
    super();
    this.tableName = tableName;
    this.metadata = metadata;
    this.tableFields = Object.keys(metadata);
    this.markParams = (markParams) ? markParams : [];

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

  getDatabaseMetadata(list) {
    return this.getMetadata(list, 'D');
  }

  /**
   * @param {string|Array<string>} list
   * @param {string} type
   */
  getMetadata(list, type) {
    if (list) {
      if (Array.isArray(list)) {
        return this.getArrayMetadata(list, type);
      } else {
        return this.getSingleMetadata(list, type);
      }
    } else {
      return this.getArrayMetadata(this.tableFields, type);
    }
  }

  getSingleMetadata(key, type) {
    return this.getMetadataInfo(key, type);
  }

  getArrayMetadata(fields, type) {
    var results = {};
    fields.forEach((key) => this.enrichMetadata(results, key, type));
    return results;
  }

  enrichMetadata(metadata, key, type) {
    var data = this.getMetadataInfo(key, type);
    if (data) {
      metadata[key] = data;
    }
  }

  getMetadataInfo(key, type) {
    var data = this.metadata[key];
    if (data) {
      if ('D' == type) {
        return JSON.parse(JSON.stringify(data.getDatabaseInfo()));
      } else if ('V' == type) {
        return JSON.parse(JSON.stringify(data.getViewInfo()));
      }
    } else {
      return null;
    }
  }

  getViewMetadata(list) {
    return this.getMetadata(list, 'V');
  }

  beforeCUD(args) {
    var exec = PgUtils.getExecutorInfo(...args);
    if (exec.params) {
      Object.keys(exec.params).forEach((key) => {
        var metadata = this.metadata[key];
        if (metadata) {
          exec.params[key] = metadata.fixValue(exec.params[key]);
        }
      });
    }
    return exec;
  }

  create() {
    var exec = this.beforeCUD(arguments);
    if (!this.isInsertableParams(exec.params)) {
      return exec.callback(null, {}, { count: 0, params: exec.params });
    }

    var builder = new InsertBuilder();
    builder.addTable(this);
    builder.setTableData(this, exec.params);
    if (this.options.useTimestamp && !exec.params.__noTimestamp) {
      builder.addRawValue('created_at', 'now()');
    }
    this.executeQuery(exec, builder.build());
  }

  isInsertableParams(params) {
    if (!_.isEmpty(params)) {
      for (var i = 0; i < this.tableFields.length; i++) {
        var key = this.tableFields[i];
        if (typeof params[key] !== 'undefined') return true;
      }
    }
    return false;
  }

  executeQuery(exec, query) {
    if (query && query.sql) {
      if (Logger.isDebug()) {
        Logger.debug(this.constructor.name + '.executeQuery', query);
      }
      exec.executor.execute(query.sql, query.params, NCommons.ok(exec.callback, function (rows, count) {
        return exec.callback(null, (rows) ? rows[0] : {}, { count: count, rows: rows, params: exec.params });
      }));
    } else {
      return exec.callback(null, {});
    }
  }

  /**
   * @param {*} executor PgUtils instance | PgTransaction
   * @param {*} params id value | { 'idField': id value,  __where, __whereRaw, __order }
   * @param {*} callback
   */
  retrive(executor, params, callback) {
    var exec = PgUtils.getExecutorInfo(...arguments);
    var obj = exec.params;
    var paramsX = {};
    if (obj === Object(obj)) {
      Object.assign(paramsX, obj);
    } else {
      paramsX[this.options.idField] = obj;
    }

    var builder = this.createSelectBuilder(paramsX);
    var query = builder.build();
    if (query && query.sql) {
      if (Logger.isDebug()) Logger.debug(this.constructor.name + '.retrive', query);
      exec.executor.selectOne(query.sql, query.params, exec.callback);
    } else {
      return exec.callback(null, {});
    }
  }

  createSelectBuilder(params) {
    var builder = new SelectBuilder();
    this.initQueryBuilder(builder, params);
    if (params.__noTimestamp) builder.addNoTimestamp(this);
    if (params.__select) builder.addSelect(this, params.__select);
    if (params.__order) builder.addOrders(this, params.__order);
    if (params.__limit) builder.setLimit(params.__limit[0], params.__limit[1]);
    return builder;
  }

  initQueryBuilder(builder, params) {
    builder.addTable(this);
    builder.setTableData(this, params);
    if (params.__where && params.__where.length) {
      params.__where.forEach((arr) => builder.addWhereObject(this, arr));
    }
    builder.setWhereRaw(params.__whereRaw);
  }

  /**
   * @param {*} executor PgUtils instance | PgTransaction
   * @param {*} params id value | { 'idField': id value,  __where, __whereRaw, __order }
   * @param {*} callback
   */
  retriveAll(executor, params, callback) {
    var exec = PgUtils.getExecutorInfo(...arguments);
    var builder = this.createSelectBuilder(exec.params);
    var query = builder.build();
    if (query && query.sql) {
      if (Logger.isDebug()) Logger.debug(this.constructor.name + '.retriveAll', query);
      exec.executor.select(query.sql, query.params, exec.callback);
    } else {
      return exec.callback(null, []);
    }
  }

  update() {
    var exec = this.beforeCUD(arguments);
    if (!this.isUpdateableParams(exec.params)) {
      return exec.callback(null, {}, { count: 0, params: exec.params });
    }

    var builder = new UpdateBuilder();
    this.initUpdateBuilder(builder, exec.params);
    if (this.options.useTimestamp && !exec.params.__noTimestamp) {
      builder.addRawValue('updated_at', 'now()');
    }
    this.executeQuery(exec, builder.build());
  }

  isUpdateableParams(params) {
    if (!_.isEmpty(params)) {
      for (var i = 0; i < this.tableFields.length; i++) {
        var key = this.tableFields[i];
        if (this.options.idField == key) continue;
        if (typeof params[key] !== 'undefined') return true;
      }
    }
    if (params.__valueRaw && params.__valueRaw.length) {
      return true;
    }
    return false;
  }

  updateChanges() {
    var exec = this.beforeCUD(arguments);
    var updateWithExisting = (existing) => {
      var params = this.getChanges(existing, exec.params);
      this.update(exec.executor, params, NCommons.ok(exec.callback, (row, extras) => {
        extras.params = params;
        return exec.callback(null, row, extras);
      }));
    };

    if (exec.params.__existing) {
      updateWithExisting(exec.params.__existing);
    } else {
      this.retrive(exec.params, NCommons.ok(exec.callback, (row) => {
        updateWithExisting(row);
      }));
    }
  }

  initUpdateBuilder(builder, params) {
    this.initUpdateBuilderX(builder, params);
    builder.addRawValues(params.__valueRaw);
  }

  initUpdateBuilderX(builder, params) {
    this.initQueryBuilder(builder, params);
    var useReturning = (params.__useReturning) ? params.__useReturning : this.options.useReturning;
    builder.setOptions('useReturning', useReturning);
  }

  delete() {
    var exec = this.beforeCUD(arguments);
    var builder = this.getDeleteBuilder(exec.params);
    this.executeQuery(exec, builder.build());
  }

  getDeleteBuilder(params) {
    var builder;
    if (this.options.useTimestamp && !params.__noTimestamp) {
      builder = new UpdateBuilder();
      builder.addRawValue('deleted_at', 'now()');
      builder.addRawValues(params.__valueRaw);
    } else {
      builder = new DeleteBuilder();
    }
    this.initUpdateBuilderX(builder, params);
    return builder;
  }

  mark() {
    var exec = this.beforeCUD(arguments);
    var builder = new UpdateBuilder();
    this.initUpdateBuilder(builder, exec.params);
    builder.addRawValues(this.markParams);
    this.executeQuery(exec, builder.build());
  }

  /**
   * return [{ field, to, key }]
   */
  getRelations() {
    return [];
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

  getFieldValue(newValues, useIdField = true) {
    var results = {};
    this.tableFields.forEach((key) => {
      var value = newValues[key];
      var isKey = (key == this.options.idField);
      if (typeof value !== 'undefined' && !isKey || (isKey && useIdField)) {
        results[key] = value;
      }
    });
    return results;
  }
}

module.exports = Crud;