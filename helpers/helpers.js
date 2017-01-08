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
                reject('could not connect');
            }
            else {

                client.query(sqlstring, (err, result) => {
                    client.end();
                    err ? reject(err) : resolve(result.rows);
                });

            } // end else (no error condition)

        }); // end client.connect

    }); // end promise

}; // end sendToDatabase
