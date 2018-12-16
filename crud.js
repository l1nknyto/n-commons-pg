const _             = require('underscore');
const PgUtils       = require('./index')();
const NCommons      = require('n-commons');
const Logger        = require('n-commons/logger');
const SelectBuilder = require('./builders/select-builder');
const InsertBuilder = require('./builders/insert-builder');
const UpdateBuilder = require('./builders/update-builder');
const DeleteBuilder = require('./builders/delete-builder');
const CrudInterface = require('./crud.interface');

class Crud extends CrudInterface
{
  /**
   * markParams: array of array(0: field, 1: value, 2: operator?)
   */
  constructor(tableName, metadata, markParams = null, options = null) {
    super();
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

  getViewMetadata(list) {
    var results = {};
    if (list) {
      list.forEach((key) => {
        var data = this.metadata[key];
        if (data) {
          results[key] = data.getViewInfo();
        }
      });
    } else {
      Object.keys(this.metadata).forEach((key) => {
        results[key] = this.metadata[key].getViewInfo();
      });
    }
    return results;
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
      return exec.callback(null, {}, { count: 0 });
    }

    var builder = new InsertBuilder();
    builder.addTable(this);
    builder.setTableData(this, exec.params);
    if (this.options.useTimestamp) {
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
      exec.executor.execute(query.sql, query.params, NCommons.ok(exec.callback, function(rows, count) {
        return exec.callback(null, (rows) ? rows[0] : {}, { count: count, rows: rows });
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
    var builder = this.createSelectBuilder((id === Object(id)) ? id : { id: id });
    var query   = builder.build();
    if (query && query.sql) {
      if (Logger.isDebug()) {
        Logger.debug(this.constructor.name + '.retrive', query);
      }
      PgUtils.selectOne(query.sql, query.params, callback);
    } else {
      return callback(null, {});
    }
  }

  createSelectBuilder(params) {
    var builder = new SelectBuilder();
    this.initQueryBuilder(builder, params);
    if (params.__noTimestamp) builder.addNoTimestamp(this);
    if (params.__select)      builder.addSelect(this, params.__select);
    if (params.__order)       builder.addOrders(this, params.__order);
    if (params.__limit)       builder.setLimit(params.__limit[0], params.__limit[1]);
    return builder;
  }

  initQueryBuilder(builder, params) {
    builder.addTable(this);
    builder.setTableData(this, params);
    if (params.__where && params.__where.length) {
      params.__where.forEach((arr) => builder.addWhereArray(this, arr));
    }
    builder.setWhereRaw(params.__whereRaw);
  }

  retriveAll(params, callback) {
    var builder = this.createSelectBuilder(params);
    var query   = builder.build();
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
    var exec = this.beforeCUD(arguments);
    if (!this.isUpdateableParams(exec.params)) {
      return exec.callback(null, {}, { count: 0 });
    }

    var builder = new UpdateBuilder();
    this.initUpdateBuilder(builder, exec.params);
    if (this.options.useTimestamp) {
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
    return false;
  }

  updateChanges() {
    var exec = this.beforeCUD(arguments);
    var updateWithExisting = (existing) => {
      var params = this.getChanges(existing, exec.params);
      this.update(exec.executor, params, NCommons.ok(exec.callback, (row, extra) => {
        extra.params = params;
        return exec.callback(null, row, extra);
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
    var returning = (params.__useReturning)
      ? params.__useReturning : this.options.useReturning;
    builder.useReturning(returning);
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