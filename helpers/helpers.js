var pg = require('pg');

//var logger = require('logger');

exports.sendToDatabase = function (sqlstring, conString, db) {

    // logger.info('SQL: ' + sqlstring);

    return new Promise(function (resolve, reject) {
        var client = new pg.Client(conString + db);

        client.connect(function (err) {

            if (err) {
                client.end();
                reject('could not connect');
            }
            else {

                client.query(sqlstring, function (err, result) {
                    client.end();
                    err ? reject(err) : resolve(result.rows);
                });

            } // end else (no error condition)

        }); // end client.connect

    }); // end promise

}; // end sendToDatabase
