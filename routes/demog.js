// returns from: http://nodejs-server-royhobbstn.c9users.io/demog?db=acs1115&schema=data&table=b19013&moe=yes&geonum=108037,108039&type=json

/*
{"source":"acs1115","schema":"data","tablemeta":[{"table_id":"b19013","table_title":"MEDIAN HOUSEHOLD INCOME IN THE PAST 12 MONTHS (IN 2015 INFLATION-ADJUSTED DOLLARS)","universe":"Universe:  Households"}],"fieldmeta":[{"column_id":"b19013001","column_title":"Median household income in the past 12 months (in 2015 Inflation-adjusted dollars)"}],"data":[{"geoname":"Eagle County, Colorado","state":"8","county":"37","place":null,"tract":null,"bg":null,"geonum":"108037","b19013001":"72214","b19013_moe001":"3789"},{"geoname":"Elbert County, Colorado","state":"8","county":"39","place":null,"tract":null,"bg":null,"geonum":"108039","b19013001":"84963","b19013_moe001":"4050"}],"error":[]}
*/

'use strict';

// TODO better variable names: start at line 153

// TODO geonum is a num
// TODO add geoid

// TODO extended geography
// TODO smart export filename CSV

// TODO lebab


// TODO error handling everywhere

// TODO interesting error case, &table=xkjd&table=sdfk creates an array of [xkjd,sdfk];
// TODO test coverage (think of everywhere where you could cause a problem with the params)
// TODO yank the database and see what happens

// TODO killer README


// BREAKING - geonum is now a number type instead of a string

var csv = require("express-csv"); // intellisense may flag this, but it is called
var logger = require('../helpers/logger');


var sendToDatabase = require('../helpers/helpers.js').sendToDatabase;


module.exports = function (app, conString) {


  app.get('/demog', function (req, res) {

    var start = process.hrtime();

    logger.info('**START**');

    // set defaults on parameters and assign them to an object called query_parameter
    var query_parameter = getParams(req, res);
    logger.info('Parameters acquired.');

    getFields() // get a full list of fields requested
      .then(function (field_list) {

        query_parameter.field = addMarginOfErrorFields(field_list);
        logger.info('Field Parameter Set');

        var promise_array = [];

        promise_array[0] = mainQuery(); // resolves to main query info

        // do not make extra calls if metadata is not wanted
        if (query_parameter.meta) {
          promise_array[1] = getFieldMeta(); // resolves to metadata info
          promise_array[2] = getTableMeta(); // resolves to table metadata info
        }
        else {
          logger.info('Skipping metadata requests.');
        }

        Promise.all(promise_array)
          .then(function (success) {
            logger.info('Ready to assemble output.');
            // this is where you combine all the info together (meta from fields 
            // and tables, plus main query) into a final JS object to be returned
            var response = assembleOutput(success);
            logger.info('--Time Elapsed: ' + process.hrtime(start)[1] / 1000000 + 'ms');
            logger.info('**COMPLETED**');

            if (query_parameter.type === 'csv') {
              // send CSV
              res.setHeader('Content-disposition', 'attachment; filename=CO_DemogExport.csv');
              res.csv(response);
            }
            else {
              // send JSON
              var pretty_print_json = query_parameter.pretty ? '  ' : '';
              response = (JSON.stringify(response, null, pretty_print_json));
              res.setHeader("Content-Type", "application/json");
              res.status(200).send(response);
            }

          }).catch(catchError);

      }).catch(catchError);


    function catchError(reason) {
      logger.error('There has been an error: ' + reason);
      logger.info('**FAILED**');
      res.status(500).send(reason);
    }



    function addMarginOfErrorFields(field_list) {

      // break the comma delimited records from field into an array  
      var field_array = field_list.split(",");

      var moe_fields = [];

      // if moe is set to yes, add the moe version of each field (push into new array, then merge with existing)
      if (query_parameter.moe) {
        logger.info('Adding margin of error fields.');

        // if text _moe doesn't already occur in the field name. funny.  keeping for backwards compatibility.  allows _moe tables in '&table=' param
        field_array.forEach(function (d) {
          if (d.indexOf('_moe') === -1) {
            moe_fields.push(d.slice(0, -3) + '_moe' + d.slice(-3));
          }
        });

        field_array = field_array.concat(moe_fields); // add in MOE fields
      }

      field_array = field_array.filter(onlyUnique);

      // send moe modified field list back to main field list
      // if there were no modifications, it was returned back the same (split/join canceled each other out)
      return field_array.join(',');

    }


    function assembleOutput(promise_output) {

      var query_data = promise_output[0]; // main data object

      if (query_parameter.meta) {
        var column_metadata = promise_output[1];
        var table_metadata = promise_output[2];
      }

      var data_array = query_data.map(function (d) {
        // convert non-null values of state, place, and county to their string equivalent
        d.state = (d.state !== null) ? (d.state).toString() : null;
        d.place = (d.place !== null) ? (d.place).toString() : null;
        d.county = (d.county !== null) ? (d.county).toString() : null;
        return d;
      });

      if (query_parameter.type === 'csv') {
        logger.info('Customizing for type CSV.');

        var data_array_as_csv_array = []; // data_array changes from array of objects to array of arrays

        data_array.forEach(function (d) {
          var keys = Object.keys(d);
          var temp_array = [];
          keys.forEach(function (e) {
            temp_array.push(d[e]);
          });
          data_array_as_csv_array.push(temp_array);
        });

        if (query_parameter.meta) {
          logger.info('Writing CSV metadata.');
          var csv_metadata_row = []; // array for csv field descriptions only

          for (var n = 0; n < column_metadata.length; n++) {
            csv_metadata_row.push(column_metadata[n].column_title);
          }

          // add metadata to front (on second row)
          csv_metadata_row.unshift("Geographic Area Name", "State FIPS", "County FIPS", "Place FIPS", "Tract FIPS", "BG FIPS", "Unique ID");

          data_array_as_csv_array.unshift(csv_metadata_row);
        }
        else {
          logger.info('Excluding metadata row from CSV.');
        }


        var field_name_row = query_parameter.field.split(",");

        // add column names to front
        field_name_row.unshift("geoname", "state", "county", "place", "tract", "bg", "geonum");
        data_array_as_csv_array.unshift(field_name_row);

        return data_array_as_csv_array;
      }
      else {
        // JSON Output (DEFAULT)
        logger.info('Customizing for type JSON');

        var json_result = {};
        json_result.source = query_parameter.db;
        json_result.schema = query_parameter.schema;

        if (query_parameter.meta) {
          logger.info('Adding JSON metadata to returned result.');
          json_result.tablemeta = table_metadata;
          json_result.fieldmeta = column_metadata;
        }
        else {
          logger.info('Excluding JSON metadata from returned result.');
        }

        json_result.data = data_array;
        return json_result;
      }


    }


    function mainQuery() {

      return new Promise(function (resolve, reject) {

        var where_clause = ""; // working string of 'where' condition to be inserted into sql query

        if (query_parameter.geonum) {
          // CASE 1:  you have a geonum.  essentially you don't care about anything else.  just get the data for that/those geonum(s)
          logger.info('CASE 1:  You have a geonum.  Just get the data for that/those geonum(s)');

          if (query_parameter.state || query_parameter.county || query_parameter.sumlev || query_parameter.geoid) {
            logger.warn('You specified either STATE, COUNTY, SUMLEV or GEOID.  These parameters are ignored when you also specify GEONUM');
          }

          where_clause = sqlList(query_parameter.geonum, 'geonum');
        }
        else if (query_parameter.geoid) {
          // CASE 2:  you have a geoid. just get the data for that/those geoid(s)
          logger.info('CASE 2: You have a geoid. just get the data for that/those geoid(s)');

          if (query_parameter.state || query_parameter.county || query_parameter.sumlev) {
            logger.warn('You specified either STATE, COUNTY or SUMLEV.  These parameters are ignored when you also specify GEOID');
          }

          var geonum_list = '1' + query_parameter.geoid.replace(/,/g, ',1'); // convert geoids to geonums
          where_clause = sqlList(geonum_list, "geonum");
        }
        else if (query_parameter.sumlev || query_parameter.county || query_parameter.state) {
          // CASE 3 - query
          logger.info('CASE 3: Query using one or all of: sumlev, county, or state.');
          var condition = getConditionString(query_parameter.sumlev, query_parameter.county, query_parameter.state);

          var countylist = query_parameter.county ? sqlList(query_parameter.county, 'county') : '';
          var statelist = query_parameter.state ? sqlList(query_parameter.state, 'state') : '';
          var sumlevlist = query_parameter.sumlev ? sqlList(query_parameter.sumlev, 'sumlev') : '';

          where_clause = constructWhereClause(condition, statelist, countylist, sumlevlist);

        }
        else {
          // CASE 4: No Geo
          logger.warn('No Geography in query.  Please specify either a geoname, geonum, sumlev, county, or state.');
          reject('No Geography in query.  Please specify either a geoname, geonum, sumlev, county, or state.');
        }

        var join_table_list = ""; // working string of tables to be inserted into sql query

        var table_array = getTableArray(query_parameter.field);

        // create a string to add to sql statement
        for (var n = 0; n < table_array.length; n++) {
          join_table_list = join_table_list + " natural join " + query_parameter.schema + "." + table_array[n];
        }

        // CONSTRUCT MAIN SQL STATEMENT
        var sql = "SELECT geoname, state, county, place, tract, bg, geonum, " + query_parameter.field + " from search." + query_parameter.schema + join_table_list + " where" + where_clause + " limit " + query_parameter.limit + ";";

        logger.info('Sending Main Query to the Database.');
        sendToDatabase(sql, conString, query_parameter.db).then(function (success) {
          logger.info('Main Query result successfully returned.');
          resolve(success);
        }, function (failure) {
          logger.error('Main Query failed.');
          reject(failure);
        });

      });
    }




    function getFields() {

      return new Promise(function (resolve, reject) {
        var field = query_parameter.field;
        var table = query_parameter.table;
        var schema = query_parameter.schema;
        var db = query_parameter.db;

        // if no fields are selected (then a table must be).  Create fields list based on the given table.
        if (!field) {
          logger.info('There are no fields in your query.  Retrieving a list of fields based upon your requested tables.');

          if (!table) {
            reject('You need to specify either table names or field names in your query.');
          }

          var table_list = sqlList(table, 'table_name');

          // Query table fields --ONLY SINGLE TABLE SELECT AT THIS TIME--
          var tablesql = "SELECT column_name from information_schema.columns where (" + table_list + ") and table_schema='" + schema + "';";

          logger.info('Sending request for a list of fields to the database.');
          var make_call_for_fields = sendToDatabase(tablesql, conString, db);

          make_call_for_fields.then(function (success) {

            if (success.length === 0) {
              reject('There are no valid field or table names in your request.');
            }

            logger.info('Successfully returned a list of fields from the database.');
            var tcolumns = []; // columns gathered from table(s?)

            for (var i = 0; i < success.length; i++) {
              if (success[i].column_name !== 'geonum') {
                tcolumns.push(success[i].column_name);
              }
            }

            field = tcolumns.join(','); // field becomes fields queried from info schema based upon table
            resolve(field);
          }, function (failure) {
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



    function getFieldMeta() {

      return new Promise(function (resolve, reject) {

        var field_array = query_parameter.field.split(",");
        var field_clause = sqlList(field_array, 'column_id');

        var field_metadata_sql = "SELECT column_id, column_verbose from " + query_parameter.schema + ".census_column_metadata where " + field_clause + ";";

        logger.info('Sending request for field metadata to the database.');

        sendToDatabase(field_metadata_sql, conString, query_parameter.db)
          .then(function (result) {

            var field_metadata_array = [];

            for (var k = 0; k < result.length; k++) {
              var temp_obj = {};
              temp_obj.column_id = result[k].column_id;
              temp_obj.column_title = result[k].column_verbose;
              field_metadata_array.push(temp_obj);
            } //end k

            logger.info('Your database request for field metadata has completed successfully.');
            resolve(field_metadata_array);

          }, function (failure) {
            logger.error('Your database request for field metadata has failed.');
            reject(failure);
          });

      }); // end new promise

    } // end getFieldMeta



    function getTableMeta() {

      return new Promise(function (resolve, reject) {

        var table_array = getTableArray(query_parameter.field);
        var table_clause = sqlList(table_array, 'table_id');

        var table_metadata_sql = "SELECT table_id, table_title, universe from " + query_parameter.schema + ".census_table_metadata where" + table_clause + ";";

        logger.info('Sending request for table metadata to the database.');

        sendToDatabase(table_metadata_sql, conString, query_parameter.db)
          .then(function (result) {

            var table_metadata_array = []; //final table metadata array

            for (var q = 0; q < result.length; q++) {
              var temp_obj = {};
              temp_obj.table_id = result[q].table_id;
              temp_obj.table_title = result[q].table_title;
              temp_obj.universe = result[q].universe;
              table_metadata_array.push(temp_obj);
            }

            logger.info('Your database request for table metadata has completed successfully.');
            resolve(table_metadata_array);

          }, function (failure) {
            logger.error('Your database request for table metadata has failed.');
            reject(failure);
          });

      }); // end new promise

    } // end getTableMeta



  }); // end route


}; // end module exports







function sqlList(string_list_or_array, field_name) {
  // convert a string list or array into a WHERE clause (using OR)

  var list_array = [];

  // will work with arrays or comma delimited strings
  if (!Array.isArray(string_list_or_array)) {
    list_array = string_list_or_array.split(",");
  }
  else {
    list_array = string_list_or_array;
  }

  var str = "";
  list_array.forEach(function (d) {
    str = str + " " + field_name + "='" + d + "' or";
  });

  // remove trailing 'or'
  return str.slice(0, -2);

}


function setDefaultSchema(db) {
  // db already validated.  setting default to 'data' ensures easier compatibility with future ACS releases.

  if (db === 'c2000' || db === 'c1990' || db === 'c1980') {
    return 'sf1';
  }
  else {
    return 'data';
  }
}


function getMOE(db, moe) {
  // if database is acs, check to see if moe option is flagged

  if (db.slice(0, 3) === 'acs') {
    if (moe === 'yes' || moe === 'y' || moe === 'true') {
      return true;
    }
  }
  return false;
}


function getPretty(pretty) {
  // pretty-print JSON option

  if (pretty === 'yes' || pretty === 'y' || pretty === 'true') {
    return true;
  }
  return false;
}

// how does this work??
function onlyUnique(value, index, self) {
  return self.indexOf(value) === index;
}


function getMeta(meta) {
  // get true/false meta parameter

  if (meta === 'no' || meta === 'n' || meta === 'false') {
    return false;
  }
  return true;
}


function getConditionString(sumlev, county, state) {
  // condition is going to be a 3 character string which identifies sumlev, county, state (yes/no) (1,0)

  var condition = "";
  sumlev ? condition = "1" : "0";
  county ? condition += "1" : condition += "0";
  state ? condition += "1" : condition += "0";

  return condition;
}


function getParams(req, res) {

  checkValidParams(req.query);

  var obj = {};

  // potential multi select (comma delimited list)
  obj.field = req.query.field;
  obj.state = req.query.state;
  obj.county = req.query.county;
  obj.geonum = req.query.geonum;
  obj.geoid = req.query.geoid;
  obj.sumlev = req.query.sumlev;
  obj.table = req.query.table;


  // potential single select
  obj.type = req.query.type || 'json';
  obj.db = req.query.db || 'acs1115';

  // set default for schema if it is missing
  obj.schema = req.query.schema || setDefaultSchema(obj.db, res);

  // by default limits to 1000 search results.  override by setting limit= in GET string
  obj.limit = parseInt(req.query.limit, 10) || 1000;

  obj.moe = getMOE(obj.db, req.query.moe);
  obj.pretty = getPretty(req.query.pretty);
  obj.meta = getMeta(req.query.meta);

  return obj;

}


function checkValidParams(query_params) {
  // get list of parameters - check all are valid

  var keys = Object.keys(query_params);

  var valid_keys = ['pretty', 'field', 'state', 'county', 'sumlev', 'place', 'geonum', 'geoid', 'db', 'schema', 'geo', 'series', 'type', 'limit', 'moe', 'table', 'meta'];

  keys.forEach(function (key) {
    if (valid_keys.indexOf(key) === -1) {
      logger.warn("Your parameter '" + key + "' is not valid.");
    }
  });

}


function constructWhereClause(condition, statelist, countylist, sumlevlist) {

  var joinlist = "";
  logger.info('Constructing WHERE clause using a combination of state, county and sumlev.');

  switch (condition) {
  case '001':
    joinlist = " (" + statelist + ") ";
    break;
  case '011':
    joinlist = " (" + countylist + ") and (" + statelist + ") ";
    break;
  case '111':
    joinlist = " (" + sumlevlist + ") and (" + countylist + ") and (" + statelist + ") ";
    break;
  case '010':
    joinlist = " (" + countylist + ") ";
    break;
  case '110':
    joinlist = " (" + sumlevlist + ") and (" + countylist + ")";
    break;
  case '100':
    joinlist = " (" + sumlevlist + ") ";
    break;
  case '101':
    joinlist = " (" + sumlevlist + ") and (" + statelist + ") ";
    break;
  }

  return joinlist;
}


function getTableArray(fields) {
  // get a list of tables based upon characters in each field name  (convention: last 3 characters identify field number, previous characters are table name) 

  var field_list = fields.split(",");

  var table_list = [];

  field_list.forEach(function (d) {
    table_list.push(d.slice(0, -3));
  });

  // remove duplicate tables in array
  table_list = table_list.filter(onlyUnique);

  return table_list;
}
