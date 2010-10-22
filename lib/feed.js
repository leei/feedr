require('./underscore');

var sys = require('sys');
var expat = require('node-expat');
var http = require('http');
var URL = require('url');
var date = require("./date.js");
var rest = require('./restler/lib/restler');
var redis = require('redis-node');

var diff = require("./diff").diff;

var seconds = 1000;
var minutes = 60*seconds;
var hours = 60*minutes;

function currTime() {
  return (new Date).getTime();
}

function FeedServer() {
  this._refreshInterval = 60;
  this._onItem = undefined;
  this._init();
}

/*
  The FeedServer manages a REDIS store with the following general schema:

  * feeds: - an ordered set of all feeds (by expires time)
  * feed:url:<url> - a string with the unique ID of the feed named
  * feed:<id> - a description of the feed
  * feed:<id>:items - an ordered list of all items in a feed (by pubDate)
  * feed:<id>:delay - the reread delay if last access failed.
  * item:<guid> - a description of the item
  * item:<guid>:feeds - a set of all feeds containing this item
*/
FeedServer.prototype = {
  _init: function () {
    var c = this.client = redis.createClient();
    c.select(1);
    this._refresh();
  },

  // The callback to use whenever an item is created or updated. With no
  // arguments returns an object with properties: f - the callback function,
  // and context - the calling context.
  //
  // The callback will be called with three arguments, the item as an Object,
  // an Array of all feed_ids this item is associated with, and a flag which
  // is true if this item is newly created.
  //
  // If the context argument is provided, then the callback will be sent to
  // this context.
  onItem: function(f, context) {
    if (f) { this._onItem = {onItem: f, context: context}; }
    return this._onItem;
  },

  // Get an object describing the named feed. The callback will be sent the
  // arguments (err, info) with err non-null describing an error status.
  feedInfo: function(feed_id, callback, context) {
    var self = this, c = this.client;
    c.get(feed_id, callback);
  },

  // Register a feed based on its URL. The callback (if provided) will be
  // given the feed_id, a REDIS key to get info about the feed.
  register: function(url, callback) {
    var self = this, c = this.client;
    var key = "feed:url:" + url;

    sys.log("fs.register " + url);
    c.get(key, function (err, feed_id) {
      if (err) return;
      if (! feed_id) {
        sys.log("fs.register no id: " + url);
        c.incr("feeds:next_id", function(err, n) {
          if (err) return;
          //sys.log("fs.register next_id=" + n);
          feed_id = "feed:id:" + n;
          sys.log("fs.register new " + url + " -> " + feed_id);
          // Associate URL with feed.
          c.set(feed_id, JSON.stringify({url: url}));
          // Associate feed_id with URL
          c.set(key, feed_id, function(err, status) {
            if (err) return;
            if (callback) { callback.call(context || self, feed_id); }
            self.maybeUpdateFeed(feed_id, url);
          });
        });
      } else {
        //sys.log("fs.register " + url + " -> " + feed_id);
        if (callback) { callback.call(context || self, feed_id); }
        self.maybeUpdateFeed(feed_id, url);
      }
    });
  },

  maybeUpdateFeed: function(feed_id, url) {
    var self = this, c = this.client;
    sys.log("fs.muf " + feed_id + " " + url);

    c.get(feed_id, function(err, info) {
      if (err) return;
      sys.log("fs.muf " + feed_id + ": " + info);
      if (info) {
        // It's a JSON string...
        info = JSON.parse(info);

        // Check for expiration.  Read if expired.
        var expires = info.expires;
        if (! expires || expires < currTime()) {
          sys.log("fs.muf " + feed_id + " EXPIRED " + expires + "<" + currTime());
          self.readFeed(feed_id, url, {etag: info.etag});
        }
      } else {
        // Initial read
        sys.log("fs.muf " + feed_id + " FIRST");
        self.readFeed(feed_id, url);
      }
    });
  },

  // Read the feed from the given URL.  Either updates or delays depending on
  // whether it was successful or not.
  readFeed: function(feed_id, url, opts) {
    var self = this, c = this.client;
    sys.log("fs.readFeed " + feed_id + " " + url);
    new Feed(url).read(function (status, data) {
      sys.log("fs.readFeed status: " + sys.inspect(status));
      if (status < 300) {
        data.url = url;
        //sys.log("fs.readFeed data: " + sys.inspect(data));
        self.updateFeed(feed_id, data);
      } else {
        self.delayFeed(feed_id);
      }
    }, opts);
  },

  // Delay refresh of the feed.  A response to an error condition, doubles the
  // delay whenever this feed was already delayed.
  delayFeed: function(feed_id) {
    var self = this, c = this.client;
    var key = feed_id + ":delay";
    c.get(key, function(err, delay) {
      if (delay) { delay = 2*JSON.parse(delay); }
      else { delay = 5*minutes; }
      sys.log("fs.delayFeed " + feed_id + " " + delay/1000 + "s");
      c.transaction(function() {
        c.set(key, delay); // Remember the delay...
        self.scheduleFeed(feed_id, currTime() + delay);
      });
    });
  },

  scheduleFeed: function(feed_id, expires) {
    var self = this, c = this.client;
    sys.log("fs.scheduleFeed " + expires + ": " + feed_id);
    c.zadd("feeds", expires, feed_id);
  },

  updateFeed: function(feed_id, feed) {
    var self = this, c = this.client;
    //sys.log("fs.uf " + feed_id + ": " + sys.inspect(feed));
    var channel = feed.channel;
    var items = channel.items;
    delete channel.items;

    // Reschedule update
    if (! feed.expires) {
      // By default, refresh once an hour.
      feed.expires = currTime() + 60*minutes;
    } else if (feed.expires < currTime() + 5*minutes) {
      // But make the minimum refresh time 5 minutes.
      feed.expires = currTime() + 5*minutes;
    }
    self.scheduleFeed(feed_id, feed.expires);

    // Update feed info and items
    c.set(feed_id, JSON.stringify(feed), function(err) {
      if (err) return;
      for (var i = 0; i < items.length; ++i) {
        //sys.log("fs.uf items[" + i + "] = " + sys.inspect(items[i]));
        self.updateItem(items[i], feed_id);
      }
    });
  },

  // The REDIS key for a given item.
  itemKey: function(item) {
    return item.guid && ("item:" + item.guid);
  },

  updateItem: function(item, feed_id) {
    var self = this, c = this.client;
    var key = self.itemKey(item);
    if (! key) { sys.log("fs.updateItem: no guid. Skip."); return; }
    var title = item.title || item.description;
    var date = (item.date || currTime());
    //sys.log("fs.updateItem " + key + " -> " + sys.inspect(title));

    // Remember all feeds containing this item
    c.sadd(key+":feeds", feed_id);

    // Save the updated item.
    c.getset(key, JSON.stringify(item), function(err, old) {
      if (err) return;
      if (old) {
        old = JSON.parse(old);
        if (! _.isEqual(old, item)) {
          var d = diff(old, item);
          if (d) {
            sys.log("fs.updateItem updated: " + date + " " + sys.inspect(title));
            sys.log("fs.updateItem diff: " + sys.inspect(d));
            //sys.log("c.zadd " + feed_id + ":items " + date + " " + key);
            c.zadd(feed_id+":items", date, key);
            self._call_onItem(item, false, d);
          }
        }
      } else {
        sys.log("fs.updateItem new: " + date + " " + sys.inspect(title));
        //sys.log("c.zadd " + feed_id + ":items " + date + " " + key);
        c.zadd(feed_id+":items", date, key);
        self._call_onItem(item, true);
      }
    });
  },

  // Whenever a new or updated item is inserted, call the registered callback.
  _call_onItem: function(item, isNew, diff) {
    var self = this, c = this.client;
    if (this._onItem) {
      c.smembers(self.itemKey(item) + ":feeds", function(err, feeds) {
        if (err) return;
        self._onItem.onItem.call(self._onItem.context || self, item, feeds, isNew, diff);
      });
    }
  },

  // This is the main "loop" which checks for new items on all registered
  // feeds based on the assumed schedule.
  _refresh: function() {
    var self = this, c = this.client;
    var now = currTime();
    // Do this every "refreshInterval"
    //
    // FIXME: This should really not be "pulsed", but instead based on a wait
    // until the next expiring feed is ready.
    sys.log("fs.refresh at " + (now + this._refreshInterval*seconds));
    this._refreshTimer = setTimeout(function() {
      self._refresh();
    }, this._refreshInterval*seconds);

    // Get all feeds that have expired and refresh them...
    sys.log("fs.refresh < " + now);
    c.zrangebyscore("feeds", 0, now, function(err, vals) {
      sys.log("fs.refresh " + sys.inspect(vals));
      _(vals).each(function (feed_id) {
        c.get(feed_id, function(err, feed) {
          if (err) return;
          feed = JSON.parse(feed);
          self.readFeed(feed_id, feed.url);
        });
      });
    });
  },

  // Stop refreshing the feeds.
  stop: function() {
    if (this._refreshTimer) {
      clearTimeout(this._refreshTimer);
      this._refreshTimer = undefined;
    }
  },

  // Query or set the refresh interval.  If secs is provided then it is
  // interpreted as the number of seconds to wait between refreshes of the
  // feeds. Returns the currently set refresh interval.
  refresh: function(secs) {
    if (secs) {
      var reset = (secs < this._refreshInterval);
      this._refreshInterval = secs;
      if (reset) {
        this.stop();
        this._refresh();
      }
    }
    return this._refreshInterval;
  }
};

var feedServer = exports.feedServer = new FeedServer();

var Feed = exports.Feed = function Feed(url) {
  this.url = url;
}

Feed.prototype = {
  read: function(callback, options) {
    var get_options = {parser: this._parse, headers: {}};
    sys.log("Feed: GET " + this.url);
    // Send the etag if provided
    if (options && options.etag) {
      sys.log("Feed: GET etag " + options.etag);
      get_options.headers.etag = options.etag;
    }
    // Make the request
    var req = rest.get(this.url, get_options);
    req.addListener('complete', function (data, response) {
      //sys.log("Feed: GOT " + sys.inspect(data));
      var headers = response.headers;
      var status = response.statusCode;
      sys.log("Feed: status " + status);
      sys.log("Feed: headers " + sys.inspect(response.headers));
      if (status >= 200 && status < 300) {
        data.lastRead = currTime();
        if (headers.expires) {
          var expires = Date.parse(headers.expires);
          data.expires = expires.getTime();
          sys.log("Feed: expires " + headers.expires + " -> " + expires.toISOString());
          sys.log("Feed: now " + (new Date).toISOString());
        }
        if (headers.etag) data.etag = headers.etag;
        // Handle TTL
        if (! data.expires) {
          var ttl = 1*hours;
          if (data.channel.ttl) { ttl = data.channel.ttl * minutes; }
          data.expires = currTime() + ttl;
          sys.log("Feed: TTL = " + ttl/1000 + "s => expires " + data.expires);
        }
      }
      callback.call(this, response.statusCode, data);
    });
  },

  _parse: function(data) {
    var p = new Parser(data);
    //sys.log("_parse.rss: " + sys.inspect(p.rss));
    return p.rss;
  }
};

function Parser(data) {
  this.current = {xmlns: {}, _content: []};
  this._stack = [];
  this._data = data;
  this.parse();
}

// TODO: Handle Atom...
Parser.prototype = {
  parse: function() {
    var p = new expat.Parser("UTF-8");
    p.addListener('startElement', _.bind(this._start, this));
    p.addListener('endElement', _.bind(this._end, this));
    p.addListener('text', _.bind(this._text, this));
    p.addListener('xmlDecl', _.bind(this._xmlDecl, this));
    p.parse(this._data);
  },

  _normalize: function() {
    var curr = this.current;
    if (curr._content.length == 0) {
      delete curr._content;
    }

    // Check for post-processing
    var p = this["_process_" + curr.name];
    if (p) { this.current = p.call(this, this.current); }
  },

  _start: function(name, attrs) {
    var head = {};
    var curr = {name: name, _content: []};

    // If there are any string attributes, then attach them.
    for (var key in attrs) {
      if (typeof key == 'string') {
        // Handle xmlns attributes
        var regex = /^xmlns:/g;
        if (regex.exec(key)) {
          if (! curr.xmlns) { curr.xmlns = _.clone(this.current.xmlns); }
          // regex.lastIndex is the start of the key
          curr.xmlns[key.substring(regex.lastIndex)] = attrs[key];
        } else {
          if (! curr.attrs) { curr.attrs = {}; }
          curr.attrs[key] = attrs[key];
        }
      }
    }
    // If there were no additions, just reuse same xmlns hash.
    if (! curr.xmlns) { curr.xmlns = this.current.xmlns; }
    this._stack.push(this.current);
    this.current = curr;

    // Check for preprocess
    var p = this["_pre_" + curr.name];
    if (p) { p.call(this, curr); }

    delete this._prev;
  },

  _end: function(name, attrs) {
    this._normalize();
    this._prev = this.current;
    this.current = this._stack.pop();
    this.current._content.push(this._prev);
  },

  _text: function(s) {
    if (this._prev && typeof this._prev == 'string') {
      var str = this.current._content.pop() + s;
      this.current._content.push(this._prev = str);
    } else if (! s.match(/^\s*$/)) {
      this.current._content.push(s);
      this._prev = s;
    }
  },

  _xmlDecl: function(version, encoding, standalone) {
  },

  _process_item: function(elem) {
    var item = { kind: 'item' };
    var contents = elem._content;
    for (var i = 0; i < contents.length; ++i) {
      var e = contents[i];
      if (e._content) {
        item[e.name] = e._content[0];
      } else {
        item[e.name] = e.attrs;
      }
    }
    //sys.log("item: " + sys.inspect(item));
    // Normalize dates
    _(["pubDate"]).each(function (name) {
      var str = item[name];
      if (str) {
        // Handle weirdo Google dates.
        if (str.match(/GMT[\+\-]00:00/)) {
          str = str.replace(/[\+\-]00:00/, "");
          sys.log("item: fix date -> " + str);
        }
        var date = Date.parse(str);
        if (! date) {
          sys.log("item: ERR date " + item[name]);
        } else {
          item[name] = date.toISOString();
          if (name == 'pubDate') {
            item.date = date.getTime();
          }
        }
      }
    });

    // Calculate the GUID if possible.
    if (! item.guid) {
      if (item.link) {
        item.guid = item.link;
      } else if (item.enclosure) {
        item.guid = item.enclosure.url;
      } else {
        sys.log("item: ERR no guid! " + sys.inspect(item));
      }
    }
    return item;
  },

  _process_channel: function(elem) {
    var chan = {items: []};
    var contents = elem._content;
    var chan_url = undefined;
    for (var i = 0; i < contents.length; ++i) {
      var e = contents[i];
      if (e.kind && e.kind == 'item') {
        // Resolve relative links here.
        if (chan_url && e.link) {
          var url = URL.parse(e.link, true);
          if (url) { e.link = URL.resolve(chan_url, url); }
        }
        //sys.log("channel items push: " + sys.inspect(e));
        chan.items.push(e);
      } else if (e.name) {
        if (e._content) {
          chan[e.name] = e._content[0];
        } else {
          chan[e.name] = e.attrs;
        }
        // Special handling of certain names...
        switch (e.name) {
        case "link":
          chan_url = URL.parse(chan[e.name]);
          break;
        }
        //sys.log("channel[" + e.name + "] = " + chan[e.name]);
      } else {
      }
    }
    return chan;
  },

  _pre_rss: function(elem) {
    this.rss = { version: elem.attrs.version };
    //sys.log("rss attrs: " + sys.inspect(elem));
  },

  _process_rss: function(elem) {
    this.rss.channel = elem._content[0];
    //sys.log("this.rss = " + sys.inspect(this.rss));
    return this.rss;
  }
};
