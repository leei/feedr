
/**
 * Module dependencies.
 */

var express = require('express');
var connect = require('connect');
var auth = require('connect-auth');
var feedServer = require('./lib/feed').feedServer;

var app = module.exports = express.createServer();

// Configuration

app.configure(function(){
  app.use(connect.cookieDecoder());
  app.use(connect.session());
  app.use(auth( [
   //auth.Twitter({consumerKey: twitterConsumerKey, consumerSecret: twitterConsumerSecret})
  ]) );

  app.set('views', __dirname + '/views');
  app.use(express.bodyDecoder());
  app.use(express.methodOverride());
  app.use(express.compiler({ src: __dirname + '/public', enable: ['less'] }));
  app.use(app.router);
  app.use(express.staticProvider(__dirname + '/public'));

  //feedServer.register("http://cyber.law.harvard.edu/rss/examples/sampleRss091.xml");
  //feedServer.register("http://cyber.law.harvard.edu/rss/examples/sampleRss092.xml");
  //feedServer.register("http://cyber.law.harvard.edu/rss/examples/rss2sample.xml");
  //feedServer.register("http://images.apple.com/main/rss/hotnews/hotnews.rss");
  //feedServer.register("http://feeds.feedburner.com/salon/greenwald?format=xml");
  feedServer.register("http://news.google.ca/news?pz=1&cf=all&ned=ca&hl=en&output=rss");
});

app.configure('development', function(){
    app.use(express.errorHandler({ dumpExceptions: true, showStack: true }));
});

app.configure('production', function(){
   app.use(express.errorHandler());
});

// Routes

app.get('/', function(req, res){
    res.render('index.jade', {
        locals: {
            title: 'Express'
        }
    });
});

// Only listen on $ node app.js

if (!module.parent) {
    app.listen(3000);
    console.log("Express server listening on port %d", app.address().port)
}
