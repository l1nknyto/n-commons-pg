const Crud         = require('../crud.interface');
const QueryBuilder = require('./query-builder');

// addUseTimestamp(table)
// addNoTimestamp(table)
// addSelect(table, field)
// addOrder(table, field, direction = 'ASC')
// addOrders(table, arr)
// setLimit(limit, offset = 0)
// --- inherit
// constructor()
// addTable(crud|{ sql, relations }, alias = '', join = 'JOIN', relations = [])
// setTableData(table, data)
// addWhere(table, field, value, operator = '=', conjuction = 'AND', rawValue = false)
// addWhereArray(table, array)
// setWhereRaw(value)
// build()
class SelectBuilder extends QueryBuilder
{
  constructor() {
    super()
    this.selects = [];
    this.orders  = [];
    this.limits  = {};
    this.tableWithTimestamp = [];
    this.tableNoTimestamp   = [];
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

  addSelect(table, field) {
    if (field instanceof Array) {
      field.forEach((f) => this._addSelectField(table, f));
    } else {
      this._addSelectField(table, field);
    }
    return this;
  }

  _addSelectField(table, field) {
    this.selects.push({
      table  : table,
      field : field
    });
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

  setLimit(limit, offset = 0) {
    this.limits = {
      limit  : limit,
      offset : offset
    };
    return this;
  }

  _buildSql() {
    return this.getCoreSql() + this.getWhereSql() + this.getOrderSql() + this.getLimitSql();
  }

  getCoreSql() {
    return 'SELECT ' + this.getSelectSql() + ' FROM '+ this.getFromSql();
  }

  getSelectSql() {
    if (!this.selects.length) {
      this.tables.forEach((value, key) => {
        this.addSelect(key, '*');
      });
    }

    var results = [];
    this.selects.forEach((item) => {
      if (item.table) {
        results = results.concat(this.getSelectField(item.table, item.field));
      } else {
        results.push(item.field);
      }
    });
    return results.join(', ');
  }

  getSelectField(table, field) {
    if (table instanceof Crud) {
      var fields = table.tableFields;
      if ('*' == field) {
        return fields.map((item) => this.getSelectFieldAs(table, item));
      } else if (fields.indexOf(field) >= 0) {
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
    return (1 == this.tables.size) ? newField : newField + ' as ' + alias + '__' + field;
  }

  getFromSql() {
    var from = '';
    this.tables.forEach((value, key) => {
      var tableAlias = (key instanceof Crud)
        ? this.getTableName(key)
        : '(' + key.sql + ') ' + value.alias;
      if (from) {
        from += ' ' + value.join + ' ' + tableAlias + this.getTableRelation(key);
      } else {
        from = tableAlias;
      }
    });
    return from;
  }

  getTableRelation(table) {
    var relations = '';
    this.tables.forEach((value, key) => {
      if (table != key) {
        if (!(relations = this._getTableRelation(table, key))) {
          relations = this._getTableRelation(key, table);
        }
      }
    });
    return relations;
  }

  _getTableRelation(table1, table2) {
    var info1 = this.tables.get(table1);
    var info2 = this.tables.get(table2);

    var relations = this._getExpectedTableRelation(table1, info1);
    for (var i = 0; i < relations.length; i++) {
      var relation = relations[i];
      if ((typeof relation.to === 'function' && table2 instanceof relation.to) || table2 == relation.to) {
        return this.getRelationCondition(relation, info1, info2);
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

  getRelationCondition(relation, info1, info2) {
    var key1 = info1.alias + '.' + relation.field;
    var key2 = info2.alias + '.' + relation.key;
    return ' ON ' + key1 + '=' + key2;
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