var sendToDatabase = require('../helpers/helpers.js').sendToDatabase;
var logger = require('../helpers/logger.js');



module.exports = function (app) {


    app.get('/plaingeo', function (req, res) {

        logger.info('Starting /plaingeo route.');


        res.status(200).send('Hello World');



    }); // end route

}; // end module
