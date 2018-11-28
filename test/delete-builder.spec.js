require('dotenv').config();

var assert = require('assert');
var expect = require('chai').expect;
var should = require('chai').should();

const TestBuilder = require('../builders/delete-builder');
const Crud        = require('../crud');

class TestCrud extends Crud
{
  constructor() {
    super('table1', { id: '', key: '', title: '' }, null, { useTimestamp: true });
  }

  getRelations() {
    return [{ field: 'key', to: TestCrud2, key: 'id' }];
  }
}

it('test build delete', function() {
  var builder = new TestBuilder();
  var table   = new TestCrud();

  builder.addTable(table);
  builder.addWhere(table, 'id', 'value 1');

  var results = builder.build();
  var expectedSql = 'DELETE FROM table1 WHERE id=$1';
  expect(results.sql).to.equal(expectedSql);
  expect(results.params).to.have.lengthOf(1);
  expect(results.params).to.deep.equal(['value 1']);
});


it('test build delete using data', function() {
  var builder = new TestBuilder();
  var table   = new TestCrud();

  builder.addTable(table);
  builder.setTableData(table, { id: 'value 1' });

  var results = builder.build();
  var expectedSql = 'DELETE FROM table1 WHERE id=$1';
  expect(results.sql).to.equal(expectedSql);
  expect(results.params).to.have.lengthOf(1);
  expect(results.params).to.deep.equal(['value 1']);
});