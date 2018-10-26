require('dotenv').config();

var assert = require('assert');
var expect = require('chai').expect;
var should = require('chai').should();

const PgUtils = require('../index')();
const Crud = require('../crud');

const METADATA = {
  id: {
    auto  : true,
    type  : 'number',
    label : 'Id'
  },
  title: {
    auto  : false,
    type  : 'text',
    label : 'Title'
  }
};

class Crud1 extends Crud
{
  constructor(tableFields) {
    super('crud1', METADATA);
  }
}

var crud1 = new Crud1();

it('test get crud metadata', function() {
  var fields = crud1.tableFields;
  fields.should.have.have.length(2);
  fields.should.have.deep.equal(['id', 'title']);
});