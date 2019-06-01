require('dotenv').config();

const Crud          = require('../crud');
const SelectBuilder = require('../builders/select-builder');

var assert = require('assert');
var expect = require('chai').expect;
var should = require('chai').should();

class TestCrud1 extends Crud
{
  constructor() {
    super('table1', { id: '', key: '', title: '' }, null, { useTimestamp: true });
  }

  getRelations() {
    return [{ field: 'key', to: TestCrud2, key: 'id' }];
  }
}

class TestCrud2 extends Crud
{
  constructor() {
    super('table2', { id: '', key: '', title: '' });
  }

  getRelations() {
    return [{ field: 'key', to: TestCrud1, key: 'id' }];
  }
}

it('test build full select feature', function() {
  var builder = new SelectBuilder();
  var table1  = new TestCrud1();
  var table2  = new TestCrud2();

  builder.addTable(table1, 'c1').addTable(table2, 'c2');
  builder.addWhere(table1, 'id', 'value 1').addWhere(table2, 'key', 'value 2');
  builder.addUseTimestamp(table1);
  builder.addSelect(table1, ['id', 'key', 'title']);
  builder.addSelect(table2, '*');
  builder.addOrder (table1, 'id', 'ASC');
  builder.addOrder (table2, 'key', 'ASC');
  builder.setLimit (10, 20);

  var results = builder.build();
  var expectedSql = 'SELECT C1.id as C1__id, C1.key as C1__key, C1.title as C1__title, C2.id as C2__id, C2.key as C2__key, C2.title as C2__title' +
    ' FROM table2 C2 JOIN table1 C1 ON C1.key=C2.id WHERE C1.id=$1 AND C2.key=$2 AND C1.deleted_at IS NULL' +
    ' ORDER BY C1.id ASC, C2.key ASC LIMIT 10 OFFSET 20';
  expect(results.sql).to.equal(expectedSql);
  expect(results.params).to.have.lengthOf(2);
  expect(results.params).to.deep.equal(['value 1', 'value 2']);
});


it('test build select using subquery', function() {
  var builder = new SelectBuilder();
  var table1  = new TestCrud1();
  var table2  = {
    sql       : 'SELECT 1 as key',
    relations : [{ field: 'id', to: TestCrud1, key: 'key'}]
  };

  builder.addTable(table1, 'c1').addTable(table2, 'c2');
  builder.addWhere(table1, 'id', 'value 1').addWhere(table2, 'key', 'value 2');
  builder.addSelect(table1, ['id', 'key', 'title']);
  builder.addSelect(table2, '*');

  var results = builder.build();
  var expectedSql = 'SELECT C1.id as C1__id, C1.key as C1__key, C1.title as C1__title, C2.*' +
    ' FROM table1 C1 JOIN (SELECT 1 as key) C2 ON C2.id=C1.key WHERE C1.id=$1 AND C2.key=$2 AND C1.deleted_at IS NULL';
  expect(results.sql).to.equal(expectedSql);
  expect(results.params).to.have.lengthOf(2);
  expect(results.params).to.deep.equal(['value 1', 'value 2']);
});


it('test build select using subquery - override relations', function() {
  var builder = new SelectBuilder();
  var table1  = new TestCrud1();
  var table2  = {
    sql : 'SELECT 1 as key'
  };

  builder.addTable(table1, 'c1', null, [{ field: 'key', to: table2, key: 'id'}]).addTable(table2, 'c2');
  builder.addSelect(table1, 'id');

  var results = builder.build();
  var expectedSql = 'SELECT C1.id as C1__id FROM (SELECT 1 as key) C2 JOIN table1 C1 ON C1.key=C2.id WHERE C1.deleted_at IS NULL';
  expect(results.sql).to.equal(expectedSql);
});


it('test build select using subquery - no relation', function() {
  var builder = new SelectBuilder();
  var table1  = new TestCrud1();
  var table2  = {
    sql : 'SELECT 1 as key'
  };

  builder.addTable(table1, 'c1').addTable(table2, 'c2');
  builder.addSelect(table1, 'id');

  var results = builder.build();
  var expectedSql = 'SELECT C1.id as C1__id FROM table1 C1,(SELECT 1 as key) C2 WHERE C1.deleted_at IS NULL';
  expect(results.sql).to.equal(expectedSql);
});


it('test build select single table', function() {
  var builder = new SelectBuilder();
  var table1  = new TestCrud1();

  builder.addTable(table1, 'c1');
  builder.addSelect(table1, '*');

  var results = builder.build();
  var expectedSql = 'SELECT C1.id, C1.key, C1.title FROM table1 C1 WHERE C1.deleted_at IS NULL';
  expect(results.sql).to.equal(expectedSql);
});


it('test build select no timestamp', function() {
  var builder = new SelectBuilder();
  var table1  = new TestCrud1();

  builder.addTable(table1, 'c1');
  builder.addNoTimestamp(table1);
  builder.addSelect(table1, 'id');

  var results = builder.build();
  var expectedSql = 'SELECT C1.id FROM table1 C1';
  expect(results.sql).to.equal(expectedSql);
});