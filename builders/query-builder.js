// constructor()
// addTable(crud|{ sql, relations }, alias = '', join = 'JOIN', relations = [])
// setTableData(table, data)
// addWhere(table, field, value, operator = '=', conjuction = 'AND', rawValue = false)
// addWhereObject(table, array)
// setWhereRaw(value)
// useReturning(value)
// build()
class QueryBuilder
{
  constructor() {
    this.tables    = new Map();
    this.tableData = new Map();
    this.whereRaw  = '';
    this.wheres    = [];
    this.params    = [];
    this.options   = {
      useReturning : null
    };
  }

  /**
   * table :
   * - crud
   * - string
   */
  addTable(table, alias = '', join = 'JOIN', relations = []) {
    var value = {
      alias     : alias.toUpperCase(),
      join      : join,
      relations : relations
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
      table      : table,
      field      : field,
      value      : value,
      operator   : (operator)   ? ' ' + operator.trim()   + ' ' : '=',
      conjuction : (conjuction) ? ' ' + conjuction.trim() + ' ' : ' AND ',
      rawValue   : (rawValue) ? rawValue : false
    };
  }

  addWhereObject(table, obj) {
    if (Array.isArray(obj)) {
      return this.addWhere(table, ...obj);
    } else {
      return this.addWhereGroup(obj.group, obj.conjuction, table);
    }
  }

  addWhereGroup(group, conjuction = null, table = null) {
    var createGroupItem = (item) => {
      if (Array.isArray(item)) {
        return this.createWhereItem(table, ...item);
      } else {
        if (!item.table) {
          item.table = table;
        }
        if (item.group) {
          item.group = item.group.map((item) => createGroupItem(item));
        }
        return item;
      }
    };
    this.wheres.push({
      group      : group.map((item) => createGroupItem(item)),
      conjuction : (conjuction) ? ' ' + conjuction.trim() + ' ' : ' AND '
    });
    return this;
  }

  setWhereRaw(value) {
    this.whereRaw = value;
    return this;
  }

  useReturning(value) {
    this.options.useReturning = value;
    return this;
  }

  build() {
    var sql = this._buildSql() + this.getReturning();
    var results = {
      sql    : sql,
      params : this.params
    };
    this.params = [];
    return results;
  }

  getReturning() {
    var useReturning = (this.options.useReturning != null)
      ? this.options.useReturning : this.getFirstTable().options.useReturning;
    return (useReturning) ? ' RETURNING *': '';
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
      var item      = group[i];
      var condition = this._createWhereItemCondition(item);
      sql = this.appendCondition(sql, condition, item.conjuction);
    }
    return '(' + sql + ')';
  }

  _createWhereSingleCondition(item) {
    var field = (item.table) ? this.getTableField(item.table, item.field) : item.field;
    return (item.rawValue)
      ? (field + item.operator + item.value)
      : this.createCondition(field, item.value, item.operator);
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
    this.params.push(value);
    return field + operator + '$' + this.params.length;
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
}

module.exports = QueryBuilder;