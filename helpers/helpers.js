'use strict';

var pg = require('pg');
var logger = require('./logger');


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
