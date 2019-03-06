const QueryBuilder = require('./query-builder');

// --- inherit
// constructor()
// addTable(crud|{ sql, relations }, alias = '', join = 'JOIN', relations = [])
// setTableData(table, data)
// addWhere(table, field, value, operator = '=', conjuction = 'AND', rawValue = false)
// addWhereObject(table, array)
// setWhereRaw(value)
// build()
class DeleteBuilder extends QueryBuilder
{
  getCoreSql() {
    return 'DELETE FROM ' + this.getFirstTableName();
  }
}

module.exports = DeleteBuilder;