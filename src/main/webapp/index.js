/**
 * https://codeforgeek.com/2015/01/render-html-file-expressjs/
 * https://gist.github.com/reu/5342276
 */

var express = require('express');
var http = require('http');

var app = express();

var redis = require("redis");
var subscriber = redis.createClient({
    host: '10.148.254.9',
     port: '6379'
}); //TODO



subscriber.subscribe("StreamingTweet");

app.use(express.static(__dirname));

app.get("/", function(req, res) {
    res.sendFile(path.join(__dirname+'/index.html'));
})

app.get("/stream", function(req, res) {
    subscriber.on("message", function(channel, message) {
        console.log("Message: " + message + "on channel: " + channel + " arrived!");

        res.write('data:' + message + '\n\n'); // Note the extra newLine is required.
    });

    res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive'
      });
      res.write('\n');

    // The 'close' event is fired when a user closes their browser window.
    // In that situation we want to make sure our redis channel subscription
    // is properly shut down to prevent memory leaks...and incorrect subscriber
    // counts to the channel.
    req.on("close", function() {
      subscriber.unsubscribe();
      subscriber.quit();
    });
})

http.createServer(app).listen(3000);
console.log("Running at Port 3000");
