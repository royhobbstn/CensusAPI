'use strict';

var pg = require('pg');
var logger = require('./logger');
var types = require('pg').types;
var fs = require("fs");



types.setTypeParser(20, function (val) {
    return parseInt(val, 10);
});

types.setTypeParser(1700, function (val) {
    return Number(val);
});


var obj = JSON.parse(fs.readFileSync("./connection.json", "utf8"));
var conString = "postgres://" + obj.name + ":" + obj.password + "@" + obj.host + ":" + obj.port + "/";



exports.sendToDatabase = function (sqlstring, db) {

    logger.info('SQL: ' + sqlstring);

    return new Promise((resolve, reject) => {

        const client = new pg.Client(conString + db);

        client.connect(err => {

            if (err) {
                client.end();
                reject('Could not connect to the database.');
            }
            else {

                client.query(sqlstring, (err, result) => {
                    client.end();
                    err ? reject(`Error in database query: ${err}`) : resolve(result.rows);
                });

            } // end else (no error condition)

        }); // end client.connect

    }); // end promise

}; // end sendToDatabase



exports.sqlList = function (string_list_or_array, field_name) {
    // convert a string list or array into a WHERE clause (using OR)

    let list_array = [];

    // will work with arrays or comma delimited strings
    if (!Array.isArray(string_list_or_array)) {
        list_array = string_list_or_array.split(",");
    }
    else {
        list_array = string_list_or_array;
    }

    let str = "";
    list_array.forEach(d => {
        str = `${str} ${field_name}='${d}' or`;
    });

    // remove trailing 'or'
    return str.slice(0, -2);

};


// http://stackoverflow.com/a/10073788
exports.pad = function (n, width, z) {
    z = z || '0';
    n = n + '';
    return n.length >= width ? n : new Array(width - n.length + 1).join(z) + n;
};



exports.setDefaultSchema = function (db) {
    // db already validated.  setting default to 'data' ensures easier compatibility with future ACS releases.

    if (db === 'c2000' || db === 'c1990' || db === 'c1980') {
        return 'sf1';
    }
    else {
        return 'data';
    }
};


exports.getMOE = function (db, moe) {
    // if database is acs, check to see if moe option is flagged

    if (db.slice(0, 3) === 'acs') {
        if (moe === 'yes' || moe === 'y' || moe === 'true') {
            return true;
        }
    }
    return false;
};


exports.getPretty = function (pretty) {
    // pretty-print JSON option

    if (pretty === 'yes' || pretty === 'y' || pretty === 'true') {
        return true;
    }
    return false;
};


exports.onlyUnique = function (value, index, self) {
    // how does this work??
    return self.indexOf(value) === index;
};


exports.getMeta = function (meta) {
    // get true/false meta parameter
    if (meta === 'no' || meta === 'n' || meta === 'false') {
        return false;
    }
    return true;
};
