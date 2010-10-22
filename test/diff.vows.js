var vows = require('vows');
var assert = require('assert');
var diff = require("./../lib/diff.js").diff;

require("./../lib/underscore.js");

vows.describe('diff').addBatch({
  'basic diff': {
    topic: diff.diff,

    'truth values are distinguished': function() {
      assert.equal(false, diff(false, false));
      assert.equal(false, diff(true, true));

      assert.equal(true, diff(false, true));
      assert.equal(true, diff(true, false));

      assert.equal(true, diff(true, 1));
      assert.equal(true, diff(false, 1));
    },

    'numbers are distinguished': function() {
      assert.equal(false, diff(1, 1));
      assert.equal(false, diff(1.0, 1.0));
      assert.equal(false, diff(1, 1.0));

      assert.equal(true, diff(1, 2));
      assert.equal(true, diff(2.0, 1));
    },

    'strings are distinguished': function() {
      assert.equal(diff('a test', 'a test'), false);
      assert.equal(diff('', ''), false);
      assert.equal(diff('', 'a test'), true);
      assert.equal(diff('a test 1', 'a test'), true);
    }
  },

  'structure diff': {
    topic: {a: 1, b: true, c: 'string'},

    'returns new elements': function(topic) {
      var added = _.clone(topic);
      added.e = 'simple';
      assert.deepEqual(diff(topic, added), {e: [undefined, 'simple']});
    },

    'returns removed elements': function(topic) {
      var removed = _.clone(topic);
      delete removed.a;
      assert.deepEqual(diff(topic, removed), {a: [1, undefined]});
    },

    'returns changed elements': function(topic) {
      var changed = _.clone(topic);
      changed.b = false;
      changed.c = 'new string';
      assert.deepEqual(diff(topic, changed), {b: [true, false], c: ['string', 'new string']});
    }
  }
}).export(module);