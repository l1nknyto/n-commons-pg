// constructor()
// setOptions(key, value)
// addTable(crud|{ sql, relations }, alias = '', join = 'JOIN', relations = [])
// setTableData(table, data)
// addWhere(table, field, value, operator = '=', conjuction = 'AND', rawValue = false)
// addWhereObject(table, array)
// addWhereGroup(group, conjuction = null, table = null)
// setWhereRaw(value)
// getTableByClass(cl)
// getTableByAlias(alias)

// build()
class QueryBuilder {

  constructor(options = null) {
    this.tables = new Map();
    this.tableData = new Map();
    this.whereRaw = '';
    this.wheres = [];
    this.params = [];
    this.options = this.initOptions(options);
  }

  initOptions(opts) {
    var options = (opts) ? opts : {};
    if (typeof options.paramIndex === 'undefined') options.paramIndex = 0;
    return options;
  }

  setOptions(key, value) {
    this.options[key] = value;
  }

  /**
   * table :
   * - crud
   * - string
   */
  addTable(table, alias = '', join = null, relations = null) {
    var value = {
      alias: alias.toUpperCase(),
      join: (join) ? join : 'JOIN',
      relations: (relations) ? relations : []
    };
    this.tables.set(table, value);
    return this;
  }

  setTableData(table, data) {
    this.tableData.set(table, data);
    return this;
  }

  addWhere(table, field, value, operator = null, conjuction = null, rawValue = null) {
    var item = this.createWhereItem(table, field, value, operator, conjuction, rawValue);
    this.wheres.push(item);
    return this;
  }

  createWhereItem(table, field, value, operator = null, conjuction = null, rawValue = null) {
    return {
      table: table,
      field: field,
      value: value,
      operator: (operator) ? ' ' + operator.trim() + ' ' : '=',
      conjuction: (conjuction) ? ' ' + conjuction.trim() + ' ' : ' AND ',
      rawValue: (rawValue) ? rawValue : false
    };
  }

  addWhereObject(table, obj) {
    if (Array.isArray(obj)) {
      return this.addWhere(table, ...obj);
    } else {
      return this.addWhereGroup(obj.group, obj.conjuction, table);
    }
  }

  /**
   * @param {any} group [ ({ table?, field, value, operator, conjuction?, group? }|[field, value, operator?, conjuction?, rawValue?])... ]
   * @param {String} conjuction
   * @param {Crud | Object} table
   */
  addWhereGroup(group, conjuction = null, table = null) {
    if (!group || !group.length) return this;
    var createGroupItem = (item, conjuction, table) => {
      if (Array.isArray(item)) {
        return this.createWhereItem(table, ...item);
      } else {
        if (!item.table) {
          item.table = table;
        }
        if (!item.conjuction) {
          item.conjuction = conjuction;
        }
        if (item.group) {
          item.group = item.group.map((i) => createGroupItem(i, item.conjuction, item.table));
        }
        return item;
      }
    };
    var conjuction_ = (conjuction) ? conjuction.trim() : 'AND';
    this.wheres.push({
      group: group.map((item) => createGroupItem(item, conjuction_, table)),
      conjuction: ' ' + conjuction_ + ' '
    });
    return this;
  }

  setWhereRaw(value) {
    this.whereRaw = value;
    return this;
  }

  build() {
    return {
      sql: this._buildSql() + this.getReturning(),
      params: this.params
    };
  }

  getReturning() {
    var optRetuning = this.options.useReturning;
    var useReturning = (typeof optRetuning !== 'undefined') ? optRetuning : this.getFirstTable().options.useReturning;
    return (useReturning) ? ' RETURNING *' : '';
  }

  _buildSql() {
    return this.getCoreSql() + this.getWhereSql();
  }

  getCoreSql() {
    throw Error('Unimplemented');
  }

  getWhereSql() {
    var sql = '';
    if (this.whereRaw) sql = this.whereRaw;
    var dataSql = this._getWhereFromTableData();
    if (dataSql) sql += (sql) ? ' AND ' + dataSql : dataSql;
    this.wheres.forEach((item) => {
      var condition = this._createWhereItemCondition(item);
      sql = this.appendCondition(sql, condition, item.conjuction);
    });
    return (sql) ? ' WHERE ' + sql : '';
  }

  _createWhereItemCondition(item) {
    if (item.group) {
      return this._createWhereGroupCondition(item.group);
    } else {
      return this._createWhereSingleCondition(item);
    }
  }

  _createWhereGroupCondition(group) {
    var sql = '';
    for (var i = 0; i < group.length; i++) {
      var item = group[i];
      var condition = this._createWhereItemCondition(item);
      sql = this.appendCondition(sql, condition, item.conjuction);
    }
    return '(' + sql + ')';
  }

  _createWhereSingleCondition(item) {
    var field = (item.table) ? this.getTableField(item.table, item.field) : item.field;
    return (item.rawValue) ?
      (field + item.operator + item.value) :
      this.createCondition(field, item.value, item.operator);
  }

  _getWhereFromTableData() {
    if (this.tableData.size) {
      this.tableData.forEach((value, key) => {
        var dataValue = value[key.options.idField];
        if (typeof dataValue !== 'undefined') {
          this.addWhere(key, key.options.idField, dataValue);
        }
      });
    } else {
      return '';
    }
  }

  createCondition(field, value, operator) {
    if (operator.indexOf('$') != -1) {
      return field + operator.replace('$', this.getPrepareParam(value));
    } else {
      return field + operator + this.getPrepareParam(value);
    }
  }

  getPrepareParam(value) {
    this.params.push(value);
    this.options.paramIndex++;
    return '$' + this.options.paramIndex;
  }

  appendCondition(sql, condition, conjuction) {
    return (sql) ? (sql + conjuction + condition) : condition;
  }

  getTableField(table, field, alias = null) {
    var newAlias = (alias) ? alias : this.tables.get(table).alias;
    return (newAlias) ? newAlias + '.' + field : field;
  }

  getTableName(table) {
    var alias = this.tables.get(table).alias;
    return (alias) ? table.tableName + ' ' + alias : table.tableName;
  }

  getFirstTable() {
    return this.tables.keys().next().value;
  }

  getFirstTableName() {
    return this.getTableName(this.getFirstTable());
  }

  getTableByClass(cl) {
    var entry;
    var itr = this.tables.entries();
    while (entry = itr.next().value) {
      if (entry[0] instanceof cl) {
        return entry[0];
      }
    }
    return null;
  }

  getTableByAlias(alias) {
    var entry;
    var itr = this.tables.entries();
    while (entry = itr.next().value) {
      if (entry[1].alias == alias) {
        return entry[0];
      }
    }
    return null;
  }
}

module.exports = QueryBuilder;
