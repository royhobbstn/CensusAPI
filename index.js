// MICROSERVICES for CensusAPI

var express = require("express");
var app = express();
var fs = require("fs");
var pg = require('pg');

var obj = JSON.parse(fs.readFileSync("./connection.json", "utf8"));
var conString = "postgres://" + obj.name + ":" + obj.password + "@" + obj.host + ":" + obj.port + "/";

var allowCrossDomain = function (req, res, next) {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Methods", "GET");
    next();
};

app.use(allowCrossDomain);

require("./routes/meta.js")(app, pg, conString);
require("./routes/demog.js")(app, conString);
require("./routes/geojson.js")(app, pg, conString);


var server = app.listen(8080, function () {
    var host = server.address().address;
    var port = server.address().port;
    console.log("Example app listening at http://", host, port);
});
