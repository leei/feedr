require('./underscore');

//var jsdiff = require("./jsdiff");

// True if they are basic types, otherwise an object describing the difference...
function diff(a, b) {
  if (_.isEqual(a, b)) { return false; }

  // Different types?
  var atype = typeof(a), btype = typeof(b);
  if (atype != btype) return true;

  // Compute string diffs...
  if (atype === 'string' && a !== b) {
    return a !== b;
    // FIXME: It would be nice if this would turn into a difference description...
    //return jsdiff.diffString(a, b);
  }

  // If a is not an object by this point, we can't handle it.
  if (atype !== 'object') return true;

  // Check for different array lengths before comparing contents.
  descr = {};
  for (var key in a) {
    if (! (key in b)) {
      descr[key] = [a[key], undefined];
    } else {
      var d = diff(a[key], b[key]);
      if (d) {
        descr[key] = [a[key], b[key]];
      }
    }
  }
  for (var key in b) {
    if (! (key in a)) {
      descr[key] = [undefined, b[key]];
    }
  }

  return descr;
};

// Export this diff function
exports.diff = diff;