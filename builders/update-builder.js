const QueryBuilder = require('./query-builder');

// addRawValue(field, value)
// --- inherit
// addUseTimestamp(table)
// addNoTimestamp(table)
// addSelect(crud, field)
// addOrder(crud, field, direction = 'ASC')
// setLimit(limit, offset = 0)
// --- inherit QueryBuilder
// constructor()
// addTable(crud|{ sql, relations }, alias = '', join = 'JOIN', relations = [])
// setTableData(table, data)
// addWhere(table, field, value, operator = '=', conjuction = 'AND', rawValue = false)
// addWhereObject(table, array)
// setWhereRaw(value)
// useReturning(value)
// build()
class UpdateBuilder extends QueryBuilder
{
  constructor() {
    super();
    this.rawValues = [];
  }

  addRawValue(field, value, operator = '=') {
    this.rawValues.push([field, value, operator]);
    return this;
  }

  addRawValues(fieldValues) {
    if (fieldValues && fieldValues.length) {
      this.rawValues = this.rawValues.concat(fieldValues);
    }
    return this;
  }

  getCoreSql() {
    var data = this.getUpdateFieldValues();
    if (data) {
      return 'UPDATE ' + this.getFirstTableName() + ' SET ' + data;
    } else {
      return '';
    }
  }

  getUpdateFieldValues() {
    var table = this.tables.keys().next().value;
    var data  = this.tableData.get(table);
    if (data && Object.keys(data).length) {
      return this._getUpdateFieldValues(table, data);
    }
    return null;
  }

  _getUpdateFieldValues(table, data) {
    var fieldValues = [];
    table.tableFields.forEach((key) => {
      if (key != table.options.idField) {
        var value = data[key];
        if (typeof value !== 'undefined') {
          fieldValues.push(key + '=$' + (this.params.length + 1));
          this.params.push(value);
        }
      }
    });
    this.rawValues.forEach((fieldValue) => {
      var key = fieldValue[0];
      if (table.tableFields.indexOf(key) != -1) {
        fieldValues.push(key + fieldValue[2] + String(fieldValue[1]));
      }
    });
    return fieldValues.join(', ');
  }
}

module.exports = UpdateBuilder;