// return all metadata for a given database/schema combination

var sendToDatabase = require('../helpers/helpers.js').sendToDatabase;
var logger = require('../helpers/logger');



module.exports = function (app) {


    app.get('/meta', function (req, res) {

        logger.info('Starting /meta route.');

        var db = req.query.db || "acs1115";
        var schema = req.query.schema || "data";

        logger.info('db: ' + db + '\nschema: ' + schema);

        var tblsql = "SELECT table_id, table_title, universe from " + schema + ".census_table_metadata;";

        sendToDatabase(tblsql, db).then(function (result) {

            logger.info('result returned from /meta call');

            var all_metadata = result.map(d => {
                var temp_obj = {};
                temp_obj.table_id = d.table_id;
                temp_obj.table_title = d.table_title;
                temp_obj.universe = d.universe;
                return temp_obj;
            });

            logger.info('data processed.  returning to user as JSON.');

            res.set({
                "Content-Type": "application/json"
            });
            res.send(JSON.stringify(all_metadata));

        }, function (error) {
            logger.info('There has been an error.');
            logger.error(error);
            res.status(500).send(error);
        });


    }); // end route

}; // end module
