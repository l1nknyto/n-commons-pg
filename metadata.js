const _ = require('underscore');

// valid value by metadata
class Metadata
{
  /**
   * databaseInfo: { key, auto, type, valid }
   * viewInfo    : { [...view engine based...] }
   */
  constructor(databaseInfo, viewInfo) {
    this.data = {
      database : databaseInfo,
      view     : viewInfo
    };
  }

  getDatabaseInfo() {
    return this.data.database;
  }

  getViewInfo() {
    var dbInfo = Object.assign({}, this.data.database);
    return Object.assign(dbInfo, this.data.view);
  }

  isNumber() {
    return ('number' == this.data.database.type);
  }

  isArray() {
    return this.data.database.type.endsWith('[]');
  }

  isJson() {
    return this.data.database.type.startsWith('json');
  }

  isText() {
    return ('text' == this.data.database.type);
  }

  fixValue(value) {
    if (value === null || typeof value === 'undefined') {
      return value;
    } else if (this.isArray()) {
      return (_.isArray(value)) ? value : [];
    } else if (this.isJson()) {
      return (_.isObject(value)) ? value : {};
    } else if (this.isText()) {
      return value;
    } else {
      var v = String(value).trim();
      if (this.isNumber()) {
        return (v) ? v : 0;
      } else {
        return (v) ? v : null;
      }
    }
  }
}

module.exports = Metadata;