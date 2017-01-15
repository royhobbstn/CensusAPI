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



exports.sendToDatabase = sendToDatabase;

function sendToDatabase(sqlstring, db) {

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

} // end sendToDatabase



exports.sqlList = sqlList;

function sqlList(string_list_or_array, field_name) {
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
}



exports.pad = pad;

function pad(n, width, z) {
    // http://stackoverflow.com/a/10073788
    z = z || '0';
    n = n + '';
    return n.length >= width ? n : new Array(width - n.length + 1).join(z) + n;
}



exports.setDefaultSchema = setDefaultSchema;

function setDefaultSchema(db) {
    // db already validated.  setting default to 'data' ensures easier compatibility with future ACS releases.
    if (db === 'c2000' || db === 'c1990' || db === 'c1980') {
        return 'sf1';
    }
    else {
        return 'data';
    }
}


exports.getMOE = getMOE;

function getMOE(db, moe) {
    // if database is acs, check to see if moe option is flagged
    if (db.slice(0, 3) === 'acs') {
        if (moe === 'yes' || moe === 'y' || moe === 'true') {
            return true;
        }
    }
    return false;
}


exports.getPretty = getPretty;

function getPretty(pretty) {
    // pretty-print JSON option

    if (pretty === 'yes' || pretty === 'y' || pretty === 'true') {
        return true;
    }
    return false;
}


function onlyUnique(value, index, self) {
    // how does this work??
    return self.indexOf(value) === index;
}


exports.getMeta = getMeta;

function getMeta(meta) {
    // get true/false meta parameter
    if (meta === 'no' || meta === 'n' || meta === 'false') {
        return false;
    }
    return true;
}



exports.getFields = getFields;

function getFields(query_parameter) {

    return new Promise((resolve, reject) => {
        let field = query_parameter.field;
        const table = query_parameter.table;
        const schema = query_parameter.schema;
        const db = query_parameter.db;

        // if no fields are selected (then a table must be).  Create fields list based on the given table.
        if (!field) {
            logger.info('There are no fields in your query.  Retrieving a list of fields based upon your requested tables.');

            if (!table) {
                reject('You need to specify either table names or field names in your query.');
            }

            const table_list = sqlList(table, 'table_name');

            // Query table fields --ONLY SINGLE TABLE SELECT AT THIS TIME--
            const table_sql = `SELECT column_name from information_schema.columns where (${table_list}) and table_schema='${schema}';`;

            logger.info('Sending request for a list of fields to the database.');

            sendToDatabase(table_sql, db)
                .then(query_result => {

                    if (query_result.length === 0) {
                        reject('There are no valid field or table names in your request.');
                    }

                    logger.info('Successfully returned a list of fields from the database.');

                    const queried_filed_list = createQueriedFieldList(query_result);

                    resolve(queried_filed_list);

                }, failure => {
                    logger.error('Your database request for a list of fields has failed.');
                    reject(failure);
                });


        }
        else {
            logger.info('You have specified field(s) in your query.');

            if (query_parameter.table) {
                logger.warn('You specified TABLE.  This parameter is ignored when you also specify FIELD');
            }
            resolve(field);
        }

    }); // end new promise

} // end get fields


function createQueriedFieldList(query_result) {
    const queried_fields = []; // columns gathered from table(s?)

    query_result.forEach(d => {
        if (d.column_name !== 'geonum') {
            queried_fields.push(d.column_name);
        }
    });

    const queried_filed_list = queried_fields.join(','); // field is updated with fields queried from info schema based upon table

    return queried_filed_list;
}


exports.addMarginOfErrorFields = addMarginOfErrorFields;

function addMarginOfErrorFields(field_list, moe) {
    // break the comma delimited records from field into an array  
    let field_array = field_list.split(",");

    const moe_fields = [];

    // if moe is set to yes, add the moe version of each field (push into new array, then merge with existing)
    if (moe) {
        logger.info('Adding margin of error fields.');

        // if text _moe doesn't already occur in the field name. funny.  keeping for backwards compatibility.  allows _moe tables in '&table=' param
        field_array.forEach(d => {
            if (d.indexOf('_moe') === -1) {
                moe_fields.push(`${d.slice(0, -3)}_moe${d.slice(-3)}`);
            }
        });

        field_array = field_array.concat(moe_fields); // add in MOE fields
    }

    field_array = field_array.filter(onlyUnique);

    // send moe modified field list back to main field list
    // if there were no modifications, it was returned back the same (split/join canceled each other out)
    return field_array.join(',');
}


exports.getTableArray = getTableArray;

function getTableArray(fields) {
    // get a list of tables based upon characters in each field name  (convention: last 3 characters identify field number, previous characters are table name) 

    const field_list = fields.split(",");

    let table_list = field_list.map(d => {
        return d.slice(0, -3);
    });

    // remove duplicate tables in array
    table_list = table_list.filter(onlyUnique);

    return table_list;
}
