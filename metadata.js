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
    var info = Object.assign({}, this.getDatabaseInfo());
    return Object.assign(info, this.data.view);
  }

  isNumber() {
    return ('number' == this.getDatabaseInfo().type);
  }

  isArray() {
    return this.getDatabaseInfo().type.endsWith('[]');
  }

  isJson() {
    return this.getDatabaseInfo().type.startsWith('json');
  }

  isText() {
    return ('text' == this.getDatabaseInfo().type);
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