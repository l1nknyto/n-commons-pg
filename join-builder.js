const PgUtils  = require('./index')(true);
const NCommons = require('n-commons');
const Logger   = require('n-commons/logger');

/**
 * example:
 * builder.crud(crud, alias, join = 'JOIN', raw = '')
 * builder.select(crud / alias, field)
 * builder.where (crud / alias, field, value, operator = '=', conjuction = 'AND')
 * builder.order (crud / alias, field, direction = 'ASC')
 * builder.limit(limit, offset)
 * builder.build
 */
class JoinBuilder
{
  constructor() {
    this.withs       = {};
    this.selects     = [];
    this.wheres      = [];
    this.whereParams = [];
    this.orders      = [];
    this.limits      = {};
  }

  /**
   * raw (sql, field, toAlias, key)
   */
  crud(crud, alias, join = 'JOIN', raw = null) {
    var key = (crud) ? crud.tableName : ('raw' + Object.keys(this.withs).length);
    this.withs[key] = {
      crud  : crud,
      alias : alias.toUpperCase(),
      join  : join,
      raw   : raw
    };
    return this;
  }

  select(crud, field) {
    if (field instanceof Array) {
      field.forEach((f) => this._addSelectField(crud, f));
    } else {
      this._addSelectField(crud, field);
    }
    return this;
  }

  _addSelectField(crud, field) {
    this.selects.push({
      crud  : crud,
      field : field
    });
  }

  where(crud, field, value, operator = '=', conjuction = 'AND') {
    this.wheres.push({
      crud       : crud,
      field      : field,
      value      : value,
      operator   : operator,
      conjuction : conjuction
    });
    return this;
  }

  order(crud, field, direction = 'ASC') {
    this.orders.push({
      crud      : crud,
      field     : field,
      direction : direction
    })
    return this;
  }

  limit(limit, offset) {
    this.limits = {
      limit  : limit,
      offset : offset
    };
    return this;
  }

  build() {
    var sql = 'SELECT ' + this.getSelectSql() + ' FROM '+ this.getFromSql();
    if (this.wheres.length) {
      sql += ' WHERE ' + this.getWhereSql();
    }
    if (this.orders.length) {
      sql += ' ORDER BY ' + this.getOrderSql();
    }
    if (this.limits.limit) {
      sql += this.getLimitSql();
    }
    return sql;
  }

  getSelectSql() {
    if (!this.selects.length) {
      for (var key in this.withs) {
        if (this.withs[key].raw) {
          this.select(this.withs[key].alias, '*');
        } else {
          this.select(this.withs[key].crud, '*');
        }
      }
    }
    var selects = [];
    this.selects.forEach((item) => {
      if (item.crud) {
        if (typeof item.crud == 'string') {
          selects.push(item.crud + '.' + item.field);
        } else {
          var crud  = item.crud;
          var key   = crud.tableName;
          var alias = this.withs[key].alias;
          if ('*' == item.field) {
            crud.tableFields.forEach((item) => {
              selects.push(crud.getField(item, alias));
            });
          } else {
            selects.push(crud.getField(item.field, alias));
          }
        }
      } else {
        selects.push(item.field);
      }
    });
    return selects.join(', ');
  }

  getFromSql() {
    var from = '';
    for (var key in this.withs) {
      var value = this.withs[key];
      if (value.raw) {
        if (from) {
          from += ' ' + value.join + ' (' + value.raw.sql + ') ' + value.alias + this.getCrudRelations(crud, value);
        } else {
          from = '(' + value.raw.sql + ') ' + value.alias;
        }
      } else {
        var crud  = value.crud;
        if (from) {
          from += ' ' + value.join + ' ' + key + ' ' + value.alias + this.getCrudRelations(crud, value);
        } else {
          from = key + ' ' + value.alias;
        }
      }
    }
    return from;
  }

  getCrudRelations(crud, value) {
    if (value.raw) {
      var key1 = value.alias + '.' + value.raw.field;
      var key2 = (value.raw.toCrud)
        ? this.getCrudField(value.raw.toCrud, value.raw.key)
        : value.raw.toAlias.toUpperCase() + '.' + value.raw.key;
      return ' ON ' + key1 + '=' + key2;
    }

    for (var key in this.withs) {
      var otherWith = this.withs[key];
      if (crud.tableName != key) {
        var relations = crud.getRelations();
        if (relations) for (var r in relations) {
          if (otherWith.crud instanceof relations[r].crud) {
            var key1 = otherWith.alias + '.' + relations[r].key;
            var key2 = this.withs[crud.tableName].alias + '.' + r;
            return ' ON ' + key1 + '=' + key2;
          }
        }
      }
    }
    return '';
  }

  getWhereSql() {
    var sql = '';
    this.wheres.forEach((item) => {
      var condition;
      var _conjuction = (item.conjuction) ? ' ' + item.conjuction.trim() + ' ' : ' AND ';
      var _operator = (item.operator) ? item.operator : '=';
      if (item.crud) {
        this.whereParams.push(item.value);
        condition = this.getCrudField(item.crud, item.field) + _operator + '$' + this.whereParams.length;
      } else {
        condition = item.field + _operator + item.value;
      }
      if (sql) {
        sql += _conjuction + condition;
      }  else {
        sql = condition;
      }
    });
    return sql;
  }

  getCrudField(crud, field) {
    return this.withs[crud.tableName].alias + '.' + field;
  }

  getOrderSql() {
    return this.orders.map((item) => {
      return this.getCrudField(item.crud, item.field) + ' ' + item.direction
    }).join(', ');
  }

  getLimitSql() {
    var sql = ' LIMIT ' + this.limits.limit;
    if (this.limits.offset) {
      sql += ' OFFSET ' + this.limits.offset;
    }
    return sql;
  }
}

module.exports = JoinBuilder;