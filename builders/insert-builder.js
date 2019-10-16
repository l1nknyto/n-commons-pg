const QueryBuilder = require('./query-builder');

// addRawValue(field, value)
// addRawValues(fieldValues)
// --- inherit
// addUseTimestamp(table)
// addNoTimestamp(table)
// addSelect(crud, field)
// addOrder(crud, field, direction = 'ASC')
// setLimit(limit, offset = 0)
// --- inherit QueryBuilder
// constructor()
// setOptions(key, value)
// addTable(crud|{ sql, relations }, alias = '', join = 'JOIN', relations = [])
// setTableData(table, data)
// addWhere(table, field, value, operator = '=', conjuction = 'AND', rawValue = false)
// addWhereObject(table, array)
// setWhereRaw(value)
// build()
class InsertBuilder extends QueryBuilder {

  constructor() {
    super();
    this.rawValues = [];
  }

  addRawValue(field, value) {
    this.rawValues.push([field, value]);
    return this;
  }

  addRawValues(fieldValues) {
    if (fieldValues && fieldValues.length) {
      this.rawValues = this.rawValues.concat(fieldValues);
    }
    return this;
  }

  _buildSql() {
    return this.getCoreSql();
  }

  getCoreSql() {
    var data = this.getInsertFieldValues();
    if (data) {
      return 'INSERT INTO ' + this.getFirstTableName() + '(' + data.fields + ') VALUES(' + data.values + ')';
    } else {
      return '';
    }
  }

  getInsertFieldValues() {
    var table = this.tables.keys().next().value;
    var data = this.tableData.get(table);
    if (data && Object.keys(data).length) {
      var results = this._getInsertFieldValues(table, data);
      if (results.values.length) {
        return results;
      }
    }
    return null;
  }

  _getInsertFieldValues(table, data) {
    var results = {
      fields: [],
      values: []
    };
    table.tableFields.forEach((key) => {
      var value = data[key];
      if (typeof value !== 'undefined') {
        results.fields.push(key);
        results.values.push(this.getPrepareParam(value));
      }
    });
    this.rawValues.forEach((fieldValue) => {
      var key = fieldValue[0];
      if (table.tableFields.indexOf(key) != -1) {
        results.fields.push(key);
        results.values.push(fieldValue[1]);
      }
    });
    return results;
  }
}

module.exports = InsertBuilder;