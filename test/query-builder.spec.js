require('dotenv').config();

var assert = require('assert');
var expect = require('chai').expect;
var should = require('chai').should();

const BuilderClass = require('../builders/query-builder');
const Crud         = require('../crud');

class TestBuilder extends BuilderClass
{
  getCoreSql() {
    return 'core-test'
  }
}

class TestCrud extends Crud
{
  constructor() {
    super('crud1', { id: '', key: '', title: '' });
  }
}

var crud = new TestCrud();

it('test build using where from table', function() {
  var builder = new TestBuilder();
  var table   = new TestCrud();

  builder.addTable(table, 'c');
  builder.addWhere(table, 'id', 'value 1');
  builder.addWhere(table, 'key', 'value 2', null, 'OR');
  builder.addWhere(table, 'title', 'should-be-escaped', null, null, true);
  builder.addWhere(null, 'now()', 'now()', null, null, true);
  builder.setWhereRaw('where-raw');
  var results = builder.build();

  expect(results.sql).to.equal('core-test WHERE where-raw AND C.id=$1 OR C.key=$2 AND C.title=should-be-escaped AND now()=now()');
  expect(results.params).to.have.lengthOf(2);
  expect(results.params).to.deep.equal(['value 1', 'value 2']);
});

it('test build using where from data', function() {
  var builder = new TestBuilder();
  var table   = new TestCrud();

  builder.addTable(table, 'c');
  builder.setTableData(table, { id: 'value 1' });
  var results = builder.build();

  expect(results.sql).to.equal('core-test WHERE C.id=$1');
  expect(results.params).to.have.lengthOf(1);
  expect(results.params).to.deep.equal(['value 1']);
});

it('test build using useReturning', function() {
  var builder = new TestBuilder();
  var table   = new TestCrud();

  builder.addTable(table, 'c');
  builder.useReturning(true);
  var results = builder.build();
  expect(results.sql).to.equal('core-test RETURNING *');
});