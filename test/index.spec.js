var assert = require('assert');
var expect = require('chai').expect;
var should = require('chai').should();

const PgUtils = require('../index')();

var table = 'tablename';
var options =  {
  fields  : ['field1', 'field2'],
  idField : 'field1',
};
var data = {
  field1 : 'value1',
  field2 : 'value2'
};

it('test getSelectSqlBindings', function() {
  var testOptions = Object.assign({
    where : [['field2', 'value2-where', '=']]
  }, options);
  var result = PgUtils.getSelectSqlBindings(table, testOptions, data);
  result.should.have.property('sql').equal('SELECT field1, field2 FROM tablename WHERE field1=$1 AND field2=$2');
  result.should.have.property('params').to.have.length(2);
  result.should.have.property('params').to.deep.equal(['value1', 'value2-where']);
});

it('test getSelectSqlBindings using whereRaw', function() {
  var testOptions = Object.assign({
    whereRaw : 'field2 = value2-where'
  }, options);
  var result = PgUtils.getSelectSqlBindings(table, testOptions, data);
  result.should.have.property('sql').equal('SELECT field1, field2 FROM tablename WHERE field2 = value2-where AND field1=$1');
  result.should.have.property('params').to.have.length(1);
  result.should.have.property('params').to.deep.equal(['value1']);
});

it('test getUpdateSqlBindings', function() {
  var testOptions = Object.assign({
    useReturning : false,
    where        : [['field2', 'value2-where', '=', 'OR']]
  }, options);
  var result = PgUtils.getUpdateSqlBindings(table, testOptions, data);
  result.should.have.property('sql').equal('UPDATE tablename SET field2=$1 WHERE field1=$2 OR field2=$3');
  result.should.have.property('params').to.have.length(3);
  result.should.have.property('params').to.deep.equal(['value2', 'value1', 'value2-where']);
});

it('test getDeleteSqlBindings', function() {
  var testOptions = Object.assign({
    useReturning : true,
    where        : [['field2', 'value2-where']]
  }, options);
  var result = PgUtils.getDeleteSqlBindings(table, testOptions, data);
  result.should.have.property('sql').equal('DELETE FROM tablename WHERE field1=$1 AND field2=$2 RETURNING *');
  result.should.have.property('params').to.have.length(2);
  result.should.have.property('params').to.deep.equal(['value1', 'value2-where']);
});