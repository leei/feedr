require('./underscore');

var sys = require('sys');
var expat = require('node-expat');
var http = require('http');
var URL = require('url');
var date = require("./date.js");
var rest = require('./restler/lib/restler');
var redis = require('redis-node');

function FeedServer() {
  this.refreshInterval = 60;
  this._start();
}

/*
  The FeedServer manages a REDIS store with the following general schema:

  * users - a set of user ids
  * user:<id> - a description of the user
  * user:<id>:feeds - the set of feeds the user subscribes to
  * feeds: - an ordered set of all feeds (by expires time)
  * feed:url:<url> - a string with the unique ID of the feed named
  * feed:<id> - a description of the feed
  * feed:<id>:users - the set of all users of this feed
  * feed:<id>:items - an ordered list of all items in a feed (by pubDate)
  * item:<guid> - a description of the item
  * item:<guid>:feeds - a set of all feeds containing this item
*/
FeedServer.prototype = {
  _start: function () {
    var c = this.client = redis.createClient();
    c.select(1);
    this.start();
  },

  register: function(url) {
    var self = this, c = this.client;
    var key = "feed:url:" + url;

    sys.log("fs.register " + url);
    c.get(key, function (err, feed_id) {
      if (err) return;
      if (! feed_id) {
        sys.log("fs.register no id");
        c.incr("feeds:next_id", function(err, n) {
          if (err) return;
          sys.log("fs.register next_id=" + n);
          feed_id = "feed:id:" + n;
          sys.log("fs.register new " + url + " -> " + feed_id);
          c.set(key, feed_id, function(err, status) {
            if (err) return;
            self.maybeUpdateFeed(feed_id, url);
          });
        });
      } else {
        //sys.log("fs.register " + url + " -> " + feed_id);
        self.maybeUpdateFeed(feed_id, url);
      }
    });
  },

  maybeUpdateFeed: function(feed_id, url) {
    var self = this, c = this.client;
    sys.log("fs.muf " + feed_id + " " + url);

    c.get(feed_id, function(err, info) {
      if (err) return;
      //sys.log("fs.muf info = " + sys.inspect(info));
      if (info) {
        // Check for expiration.  Read if expired.
        var expires = info.expires;
        if (! expires || expires < Date.now().getTime()) {
          self.readFeed(feed_id, url);
        }
      } else {
        // Initial read
        self.readFeed(feed_id, url);
      }
    });
  },

  readFeed: function(feed_id, url) {
    var self = this, c = this.client;
    sys.log("fs.readFeed " + feed_id + " " + url);
    new Feed(url).read(function (status, data) {
      sys.log("fs.readFeed status: " + sys.inspect(status));
      if (status == 200) {
        //sys.log("fs.readFeed data: " + sys.inspect(data));
        self.updateFeed(feed_id, data);
      }
    });
  },

  updateFeed: function(feed_id, feed) {
    var self = this, c = this.client;
    sys.log("fs.uf " + sys.inspect(feed));
    var channel = feed.channel;
    var items = channel.items;
    delete channel.items;

    c.getset(feed_id, JSON.stringify(feed), function(err, old) {
      if (err) return;
      if (feed.expires) {
        sys.log("fs.uf " + feed_id + " score " + feed.expires);
        //sys.log("c.zadd feeds " + feed.expires + " " + feed_id);
        c.zadd("feeds", feed.expires, feed_id);
      }
      for (var i = 0; i < items.length; ++i) {
        //sys.log("fs.uf items[" + i + "] = " + sys.inspect(items[i]));
        self.updateItem(items[i], feed_id);
      }
    });
  },

  updateItem: function(item, feed_id) {
    var self = this, c = this.client;
    var guid = item.guid;
    if (! guid) { sys.log("fs.updateItem: no guid. Skip."); return; }
    var title = item.title || item.description;
    sys.log("fs.updateItem " + sys.inspect(title) + " guid:" + guid);
    var key = "item:" + guid;
    var date = (item.date || Date.now());
    sys.log("fs.updateItem " + key + " -> " + sys.inspect(title));
    //sys.log("fs.updateItem date = " + date);
    c.getset(key, JSON.stringify(item), function(err, old) {
      if (err) return;
      if (old) {
        old = JSON.parse(old);
        if (! _.isEqual(item, old)) {
          sys.log("updated item: " + date + " " + sys.inspect(title));
          //sys.log(" item: " + sys.inspect(item));
          //sys.log("  was: " + sys.inspect(old));
          //sys.log("c.zadd " + feed_id + ":items " + date + " " + key);
          c.zadd(feed_id+":items", date, key);
        }
      } else {
        sys.log("new item: " + date + " " + sys.inspect(title));
        //sys.log("c.zadd " + feed_id + ":items " + date + " " + key);
        c.zadd(feed_id+":items", date, key);
      }
    });
  },

  start: function() {
    var self = this, c = this.client;
    var now = Date.now();
    // Do this every minute...
    sys.log("fs.start refresh at " + now + this.refreshInterval);
    this.refresh = setTimeout(function() { self.start(); }, this.refreshInterval*1000);
    // Get all feeds that will expire in the next minute, and refresh them...
    sys.log("fs.start < " + now);
    c.zrange("feeds", 0, now, function(err, vals) {
      sys.log("fs.start " + sys.inspect(vals));
    });
  },

  stop: function() {
    if (this.refresh) {
      clearTimeout(this.refresh);
      this.refresh = undefined;
    }
  },

  refresh: function(secs) {
    var reset = (secs < this.refreshInterval);
    this.refreshInterval = secs;
    if (reset) {
      this.stop();
      this.start();
    }
  }
};

var feedServer = exports.feedServer = new FeedServer();

var Feed = exports.Feed = function Feed(url) {
  this.url = url;
}

Feed.prototype = {
  read: function(callback) {
    sys.log("Feed: GET " + this.url);
    var req = rest.get(this.url, {parser: this._parse});
    req.addListener('complete', function (data, response) {
      //sys.log("Feed: GOT " + sys.inspect(data));
      var headers = response.headers;
      var status = response.statusCode;
      sys.log("Feed: status " + status);
      sys.log("Feed: headers " + sys.inspect(response.headers));
      if (status >= 200 && status < 300) {
        data.lastRead = Date.now();
        if (headers.expires) data.expires = Date.parse(headers.expires).getTime();
        if (headers.etag) data.etag = headers.etag;
        // Handle TTL
        if (! data.expires) {
          var ttl = 1000*60*60;
          if (data.channel.ttl) { ttl = data.channel.ttl * 1000*60; }
          data.expires = Date.now() + ttl;
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
    sys.log("item: " + sys.inspect(item));
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
