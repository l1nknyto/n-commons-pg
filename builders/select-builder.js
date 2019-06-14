const _            = require('underscore');
const Crud         = require('../crud.interface');
const QueryBuilder = require('./query-builder');

// addUseTimestamp(table)
// addNoTimestamp(table)
// addSelect(table, field, raw = false)
// addOrder(table, field, direction = 'ASC')
// addOrders(table, arr)
// addGroupBy(table, field)
// setLimit(limit, offset = 0)
// getResultField(table, field)
// initSelectFields()
// --- inherit
// constructor()
// addTable(crud|{ sql, relations }, alias = '', join = 'JOIN', relations = [])
// setTableData(table, data)
// addWhere(table, field, value, operator = '=', conjuction = 'AND', rawValue = false)
// addWhereObject(table, array)
// setWhereRaw(value)
// build()
class SelectBuilder extends QueryBuilder
{
  constructor(options) {
    super();
    this.selects = [];
    this.orders  = [];
    this.groupBy = [];
    this.limits  = {};
    this.tableWithTimestamp = [];
    this.tableNoTimestamp   = [];
    this.options = this.initOptions(options);
  }

  initOptions(opts) {
    var options = (opts) ? opts : {};
    if (typeof options.useSelectAs === 'undefined') options.useSelectAs = true;
    return options;
  }

  addUseTimestamp(table) {
    if (table.options.useTimestamp) {
      this.tableWithTimestamp.push(table);
    }
    return this;
  }

  addNoTimestamp(table) {
    this.tableNoTimestamp.push(table);
    return this;
  }

  addSelect(table, field, raw = false) {
    if (field instanceof Array) {
      field.forEach((f) => this._addSelectField(table, f, raw));
    } else {
      this._addSelectField(table, field, raw);
    }
    return this;
  }

  _addSelectField(table, field, raw = false) {
    if ('*' == field && table instanceof Crud) {
      table.tableFields.forEach((field) => this.addSelect(table, field));
    } else {
      this.selects.push({ table: table, field: field, raw: raw });
    }
  }

  addOrder(table, field, direction = 'ASC') {
    this.orders.push({
      table      : table,
      field     : field,
      direction : direction
    });
    return this;
  }

  addOrders(table, arr) {
    if (arr && arr.length) {
      arr.forEach((order) => {
        this.orders.push({
          table     : table,
          field     : order[0],
          direction : (order[1]) ? order[1] : 'ASC'
        });
      });
    }
    return this;
  }

  addGroupBy(table, field) {
    if (field instanceof Array) {
      field.forEach((f) => this.addGroupBy(table, f));
    } else {
      this.groupBy.push({ table: table, field: field });
    }
    return this;
  }

  setLimit(limit, offset = 0) {
    this.limits = {
      limit  : limit,
      offset : offset
    };
    return this;
  }

  getReturning() {
    return '';
  }

  _buildSql() {
    return this.getCoreSql()
      + this.getWhereSql()
      + this.getGroupBySql()
      + this.getOrderSql()
      + this.getLimitSql();
  }

  getCoreSql() {
    return 'SELECT ' + this.getSelectSql() + ' FROM '+ this.getFromSql();
  }

  initSelectFields() {
    if (!this.selects.length) {
      this.tables.forEach((info, table) => {
        this.addSelect(table, '*');
      });
    }
  }

  getSelectSql() {
    var results = [];
    this.initSelectFields();
    this.selects.forEach((item) => {
      if (item.raw) {
        results.push(item.field);
      } else if (item.table) {
        results = results.concat(this.getSelectField(item.table, item.field));
      } else {
        results.push(item.field);
      }
    });
    return results.join(', ');
  }

  getSelectField(table, field) {
    if (table instanceof Crud) {
      if (table.tableFields.indexOf(field) >= 0) {
        return [this.getSelectFieldAs(table, field)];
      } else {
        return [];
      }
    } else {
      if ('*' == field) {
        return [this.getTableField(table, field)];
      } else {
        return [this.getSelectFieldAs(table, field)];
      }
    }
  }

  getSelectFieldAs(table, field) {
    var alias = this.tables.get(table).alias;
    var newField = this.getTableField(table, field, alias);
    return (this.options.useSelectAs && this.tables.size > 1) ? newField + ' as ' + alias + '__' + field : newField;
  }

  getResultField(table, field) {
    var alias = this.tables.get(table).alias;
    var resultField = (this.options.useSelectAs && this.tables.size > 1) ? alias + '__' + field : field;
    return resultField.toLowerCase();
  }

  getResultRow(row, table, field) {
    if (Array.isArray(field)) {
      var results = {};
      for (var i = 0; i < field.length; i++) {
        var f = field[i];
        if (Array.isArray(f)) {
          results[f[1]] = row[this.getResultField(table, f[0])];
        } else {
          results[f] = row[this.getResultField(table, f)];
        }
      }
      return results;
    } else {
      return row[this.getResultField(table, field)];
    }
  }

  getFromSql() {
    var table;
    var tableHasRelation =[], tableJoineds = [], tableRelations = [];
    var itr = this.tables.keys();
    while(table = itr.next().value) {
      this._addTableRelation(table, tableHasRelation, tableJoineds, tableRelations);
    }

    var from = [];
    this.tables.forEach((info, table) => {
      if (tableHasRelation.indexOf(table) == -1) {
        var tableAlias = (table instanceof Crud)
          ? this.getTableName(table) : '(' + table.sql + ') ' + info.alias;
        from.push(tableAlias);
      }
    });

    for (var i = 0; i < tableJoineds.length; i++) {
      var sqlTableJoin = [];
      for (var j = 0; j < tableJoineds[i].length; j++) {
        var tableJoin  = tableJoineds[i][j];
        var relation   = tableRelations[i][j];
        var info       = this.tables.get(tableJoin);
        var tableAlias = (tableJoin instanceof Crud)
          ? this.getTableName(tableJoin) : '(' + tableJoin.sql + ') ' + info.alias;
        if (relation) {
          sqlTableJoin.push(info.join + ' ' + tableAlias + relation);
        } else {
          sqlTableJoin.push(tableAlias);
        }
      }
      from.push(sqlTableJoin.join(' '));
    }
    return from.join(',');
  }

  _addTableRelation(table, tableHasRelation, tableJoineds, tableRelations) {
    var otherTable;
    var itr = this.tables.keys();
    while(otherTable = itr.next().value) {
      if (table == otherTable) continue;
      if (this._inTableJoined(otherTable, tableJoineds) != -1) continue;

      var relation;
      if (relation = this._getTableRelation(table, otherTable)) {
        this._addTableJoined(otherTable, table, relation, tableHasRelation, tableJoineds, tableRelations);
      } else if (relation = this._getTableRelation(otherTable, table)) {
        this._addTableJoined(table, otherTable, relation, tableHasRelation, tableJoineds, tableRelations);
      }
    }
  }

  _getTableRelation(table1, table2) {
    var info1 = this.tables.get(table1);
    var info2 = this.tables.get(table2);
    var relations = this._getExpectedTableRelation(table1, info1);
    for (var i = 0; i < relations.length; i++) {
      var relation = relations[i];
      if (table2 == relation.to || (_.isFunction(relation.to) && table2 instanceof relation.to)) {
        return this._getRelationCondition(relation, info1, info2);
      }
    }
    return '';
  }

  _getExpectedTableRelation(table1, info1) {
    if (info1.relations && info1.relations.length) {
      return info1.relations;
    } else if (table1 instanceof Crud) {
      return table1.getRelations();
    } else if (table1.relations) {
      return table1.relations;
    } else {
      return [];
    }
  }

  _getRelationCondition(relation, info1, info2) {
    var key1 = info1.alias + '.' + relation.field;
    var key2 = info2.alias + '.' + relation.key;
    return ' ON ' + key1 + '=' + key2;
  }

  _inTableJoined(table, tableJoineds) {
    for (var i = 0; i < tableJoineds.length; i++) {
      var index = tableJoineds[i].indexOf(table);
      if (index != -1) return i;
    }
    return -1;
  }

  _addTableJoined(table, otherTable, relation, tableHasRelation, tableJoineds, tableRelations) {
    var i = this._inTableJoined(table, tableJoineds);
    if (i != -1) {
      tableJoineds[i].push(otherTable);
      tableRelations[i].push(relation);
    } else {
      var j = this._inTableJoined(otherTable, tableJoineds);
      if (j != -1) {
        tableJoineds[j].push(table);
        tableRelations[j].push(relation);
      } else {
        tableJoineds.push([table, otherTable]);
        tableRelations.push(['', relation]);
      }
    }

    if (tableHasRelation.indexOf(table) == -1) tableHasRelation.push(table);
    if (tableHasRelation.indexOf(otherTable) == -1) tableHasRelation.push(otherTable);
  }

  getWhereSql() {
    var tables = this.getTableWithTimestamp();
    if (tables && tables.length) {
      tables.forEach((table) => {
        this.addWhere(table, 'deleted_at', 'NULL', 'IS', null, true);
      });
    }
    return super.getWhereSql();
  }

  getTableWithTimestamp() {
    if (!this.tableWithTimestamp.length) {
      this.tables.forEach((value, key) => {
        if (key instanceof Crud && (this.tableNoTimestamp.indexOf(key) == -1) && key.options.useTimestamp) {
          this.tableWithTimestamp.push(key);
        }
      });
    }
    return this.tableWithTimestamp;
  }

  getOrderSql() {
    if (!this.orders.length) return '';
    return ' ORDER BY ' + this.orders.map((item) => {
      return this.getTableField(item.table, item.field) + ' ' + item.direction;
    }).join(', ');
  }

  getGroupBySql() {
    if (!this.groupBy.length) return '';
    return ' GROUP BY ' + this.groupBy.map((item) => {
      return this.getTableField(item.table, item.field);
    }).join(', ');
  }

  getLimitSql() {
    var sql = '';
    if (this.limits.limit) {
      sql += ' LIMIT ' + this.limits.limit;
    }
    if (this.limits.offset) {
      sql += ' OFFSET ' + this.limits.offset;
    }
    return sql;
  }
}

module.exports = SelectBuilder;