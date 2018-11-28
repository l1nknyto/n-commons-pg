require('dotenv').config();

var assert = require('assert');
var expect = require('chai').expect;
var should = require('chai').should();

const TestBuilder = require('../builders/insert-builder');
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

it('test build insert', function() {
  var builder = new TestBuilder();
  var table   = new TestCrud();

  builder.addTable(table);
  builder.setTableData(table, { key: 'key 1', title: 'title 1', __to_be_ignore: 'ignored' });

  var results = builder.build();
  var expectedSql = 'INSERT INTO table1(key,title) VALUES($1,$2)';
  expect(results.sql).to.equal(expectedSql);
  expect(results.params).to.have.lengthOf(2);
  expect(results.params).to.deep.equal(['key 1', 'title 1']);
});


it('test build insert with raw value', function() {
  var builder = new TestBuilder();
  var table   = new TestCrud();

  builder.addTable(table);
  builder.setTableData(table, { key: 'key 1' });
  builder.addRawValue('title', 'now()');

  var results = builder.build();
  var expectedSql = 'INSERT INTO table1(key,title) VALUES($1,now())';
  expect(results.sql).to.equal(expectedSql);
  expect(results.params).to.have.lengthOf(1);
  expect(results.params).to.deep.equal(['key 1']);
});