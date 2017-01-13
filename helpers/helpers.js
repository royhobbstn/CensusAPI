'use strict';

var pg = require('pg');
var logger = require('./logger');
var types = require('pg').types;


types.setTypeParser(20, function (val) {
    return parseInt(val, 10);
});

types.setTypeParser(1700, function (val) {
    return Number(val);
});


exports.sendToDatabase = function (sqlstring, conString, db) {

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
                    err ? reject(`Error in databas query: ${err}`) : resolve(result.rows);
                });

            } // end else (no error condition)

        }); // end client.connect

    }); // end promise

}; // end sendToDatabase
