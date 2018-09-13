require('dotenv').config()

var assert = require('assert');
var expect = require('chai').expect;
var should = require('chai').should();

const PgUtils          = require('../index')();
const Crud             = require('../crud');
const JoinBuilderClass = require('../join-builder');

class Crud1 extends Crud
{
  constructor(tableFields, markRawParams, options = null) {
    super('crud1', ['id', 'key', 'title'], null, null);
  }

  getRelations() {
    return {
      'key' : { crud: Crud2, key: 'id' }
    };
  }
}

class Crud2 extends Crud
{
  constructor(tableFields, markRawParams, options = null) {
    super('crud2', ['id', 'key', 'title'], null, null);
  }

  getRelations() {
    return {
      'id' : { crud: Crud1, key: 'key' }
    };
  }
}

var crud1 = new Crud1();
var crud2 = new Crud2();

it('test join using crud', function() {
  var builder = new JoinBuilderClass();
  builder.crud(crud1, 'c1');
  builder.crud(crud2, 'c2');
  expect(builder.build()).to.contain('FROM crud1 C1 JOIN crud2 C2 ON C1.key=C2.id');
});

it('test join using subquery to Crud', function() {
  var builder = new JoinBuilderClass();
  builder.crud(crud1, 'c1');
  builder.crud(null, 'c2', 'join', {
    sql     : 'SELECT 1 as key',
    field   : 'key',
    toCrud  : crud1,
    key     : 'id'
  });
  expect(builder.build()).to.contain('FROM crud1 C1 join (SELECT 1 as key) C2 ON C2.key=C1.id');
});

it('test join using subquery to subquery', function() {
  var builder = new JoinBuilderClass();
  builder.crud(null, 'c1', 'join', {
    sql     : 'SELECT 1 as id',
    field   : 'id',
    toAlias : 'c2',
    key     : 'key'
  });
  builder.crud(null, 'c2', 'join', {
    sql     : 'SELECT 2 as key',
    field   : 'key',
    toAlias : 'c1',
    key     : 'id'
  });
  expect(builder.build()).to.contain('FROM (SELECT 1 as id) C1 join (SELECT 2 as key) C2 ON C2.key=C1.id');
});

it('test default select', function() {
  var builder = new JoinBuilderClass();
  builder.crud(crud1, 'c1');
  builder.crud(crud2, 'c2');
  expect(builder.build()).to.contain('SELECT C1.id AS C1__id, C1.key AS C1__key, C1.title AS C1__title, C2.id AS C2__id, C2.key AS C2__key, C2.title AS C2__title FROM');
});

it('test select using field', function() {
  var builder = new JoinBuilderClass();
  builder.crud(crud1, 'c1');
  builder.crud(crud2, 'c2');
  builder.select(crud1, 'id').select(crud2, 'key');
  expect(builder.build()).to.contain('SELECT C1.id AS C1__id, C2.key AS C2__key FROM');
});

it('test where', function() {
  var builder = new JoinBuilderClass();
  builder.crud(crud1, 'c1');
  builder.crud(crud2, 'c2');
  builder.where(crud1, 'id', '100').where(crud2, 'key', 'value')
  expect(builder.build()).to.contain('WHERE C1.id=$1 AND C2.key=$2');

  var params = builder.whereParams;
  params.should.have.have.length(2);
  params.should.have.deep.equal(['100', 'value']);
});

it('test order', function() {
  var builder = new JoinBuilderClass();
  builder.crud(crud1, 'c1');
  builder.crud(crud2, 'c2');
  builder.order(crud1, 'id', 'DESC');
  expect(builder.build()).to.contain('ORDER BY C1.id DESC');
});

it('test limit', function() {
  var builder = new JoinBuilderClass();
  builder.crud(crud1, 'c1');
  builder.crud(crud2, 'c2');
  builder.limit(10, 30);
  expect(builder.build()).to.contain('LIMIT 10 OFFSET 30');
});