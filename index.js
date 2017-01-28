// MICROSERVICES for CensusAPI

var express = require("express");
var app = express();


var allowCrossDomain = function (req, res, next) {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Methods", "GET");
    next();
};


app.use(allowCrossDomain);


require("./routes/meta.js")(app);
require("./routes/demog.js")(app);
require("./routes/geojson.js")(app);
require("./routes/plaingeo.js")(app);


var server = app.listen(8080, function () {
    var host = server.address().address;
    var port = server.address().port;
    console.log("Example app listening at http://", host, port);
});
