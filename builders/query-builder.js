// constructor()
// addTable(crud|{ sql, relations }, alias = '', join = 'JOIN', relations = [])
// setTableData(table, data)
// addWhere(table, field, value, operator = '=', conjuction = 'AND', rawValue = false)
// addWhereArray(table, array)
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
      useReturning : false
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
    this.wheres.push({
      table      : table,
      field      : field,
      value      : value,
      operator   : (operator) ? operator : '=',
      conjuction : (conjuction) ? ' ' + conjuction.trim() + ' ' : ' AND ',
      rawValue   : (rawValue) ? rawValue : false
    });
    return this;
  }

  addWhereArray(table, arr) {
    return this.addWhere(table, arr[0], arr[1], arr[2], arr[3], arr[4]);
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
    var sql = this._buildSql() + (this.options.useReturning ? ' RETURNING *': '');
    var results = {
      sql    : sql,
      params : this.params
    };
    this.params = [];
    return results;
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
      var field = (item.table) ? this.getTableField(item.table, item.field) : item.field;
      var condition = (item.rawValue)
        ? field + ' ' + item.operator + ' ' + item.value
        : this.createCondition(field, item.value, item.operator);
      sql = this.appendCondition(sql, condition, item.conjuction);
    });
    return (sql) ? ' WHERE ' + sql : '';
  }

  _getWhereFromTableData() {
    if (this.tableData.size) {
      this.tableData.forEach((value, key) => {
        var dataValue = value[key.options.idField];
        if (dataValue) {
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
    return (sql) ? sql + conjuction + condition : condition;
  }

  getTableField(table, field, alias = null) {
    var newAlias = (alias) ? alias : this.tables.get(table).alias;
    return (newAlias) ? newAlias + '.' + field : field;
  }

  getTableName(table) {
    var alias = this.tables.get(table).alias;
    return (alias) ? table.tableName + ' ' + alias : table.tableName;
  }

  getFirstTableName() {
    return this.getTableName(this.tables.keys().next().value);
  }
}

module.exports = QueryBuilder;