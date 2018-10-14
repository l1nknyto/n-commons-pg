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
    this.withs         = {};
    this.crudTimestamp = [];
    this.selects       = [];
    this.wheres        = [];
    this.whereRaw      = '';
    this.whereParams   = [];
    this.orders        = [];
    this.limits        = {};
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

  useTimestamp(crud) {
    if (crud.options.useTimestamp) {
      this.crudTimestamp.push(crud);
    }
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

  whereRaw(value) {
    this.whereRaw = value;
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

    var timestampCruds = this.getCrudUseTimestamp();
    if (timestampCruds) {
      var deleteAt = this.getDeleteAtCondition(timestampCruds);
      this.whereRaw = (this.whereRaw) ? this.whereRaw + ' AND ' + deleteAt : deleteAt;
    }

    if (this.wheres.length || this.whereRaw) {
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

  getCrudUseTimestamp() {
    if (!this.crudTimestamp.length) {
      for (var key in this.withs) {
        var crud = this.withs[key].crud;
        if (crud.options.useTimestamp) {
          this.crudTimestamp.push(crud);
        }
      }
    }
    return this.crudTimestamp;
  }

  getDeleteAtCondition(cruds) {
    var deleteAts = []
    for (var i = 0; i < cruds.length; i++) {
      var crud = cruds[i];
      deleteAts.push(this.withs[crud.tableName].alias + '.' + crud.getDeleteAtCondition());
    }
    return deleteAts.join(' AND ');
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
        var relationCondition;
        if (relationCondition = this._getCrudRelations(crud.tableName, key)) {
          return relationCondition;
        } else if (relationCondition = this._getCrudRelations(key, crud.tableName)) {
          return relationCondition;
        }
      }
    }
    return '';
  }

  _getCrudRelations(tableName1, tableName2) {
    var with1 = this.withs[tableName1];
    var with2 = this.withs[tableName2];

    var relations = with1.crud.getRelations();
    if (relations) for (var r in relations) {
      if (with2.crud instanceof relations[r].crud) {
        var key1 = with1.alias + '.' + r;
        var key2 = with2.alias + '.' + relations[r].key;
        return ' ON ' + key1 + '=' + key2;
      }
    }
    return null;
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
    return sql + (this.whereRaw ? ' AND ' + this.whereRaw : this.whereRaw);
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