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
// setOptions(key, value)
// addTable(crud|{ sql, relations }, alias = '', join = 'JOIN', relations = [])
// setTableData(table, data)
// addWhere(table, field, value, operator = '=', conjuction = 'AND', rawValue = false)
// addWhereObject(table, array)
// setWhereRaw(value)
// build()
class UpdateBuilder extends QueryBuilder {

  constructor() {
    super();
    this.rawValues = [];
  }

  addRawValue(field, value, operator = null) {
    this.rawValues.push([field, value, (operator) ? operator : '=']);
    return this;
  }

  addRawValues(fieldValues) {
    if (fieldValues && fieldValues.length) {
      fieldValues.forEach((fv) => {
        this.addRawValue(fv[0], fv[1], fv[2]);
      });
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
    var data = this.tableData.get(table);
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
          fieldValues.push(key + '=' + this.getPrepareParam(value));
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