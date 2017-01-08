//returns from: http://nodejs-server-royhobbstn.c9users.io/demog?db=acs1115&schema=data&table=b19013&moe=yes&geonum=108037,108039&type=json
/*
{"source":"acs1115","schema":"data","tablemeta":[{"table_id":"b19013","table_title":"MEDIAN HOUSEHOLD INCOME IN THE PAST 12 MONTHS (IN 2015 INFLATION-ADJUSTED DOLLARS)","universe":"Universe:  Households"}],"fieldmeta":[{"column_id":"b19013001","column_title":"Median household income in the past 12 months (in 2015 Inflation-adjusted dollars)"}],"data":[{"geoname":"Eagle County, Colorado","state":"8","county":"37","place":null,"tract":null,"bg":null,"geonum":"108037","b19013001":"72214","b19013_moe001":"3789"},{"geoname":"Elbert County, Colorado","state":"8","county":"39","place":null,"tract":null,"bg":null,"geonum":"108039","b19013001":"84963","b19013_moe001":"4050"}],"error":[]}
*/

'use strict';

// TODO super logging
// TODO different logging levels?
// TODO should not even make metadata calls if not wanted

// TODO what happens if i just THROW an error somewhere?
// TODO better variable names
// TODO error handling everywhere
// TODO time request

// TODO option disable metadata
// TODO extended geography

// TODO geonum is a num
// TODO add geoid

// TODO lebab

// TODO test coverage (think of everywhere where you could cause a problem with the params)
// TODO yank the database and see what happens


var csv = require("express-csv");
var logger = require('../helpers/logger');


var sendToDatabase = require('../helpers/helpers.js').sendToDatabase;


module.exports = function (app, conString) {


  app.get('/demog', function (req, res) {

    logger.info('**START**');

    // if the query is bad, make a quick exit
    immediateErrors(req, res);
    logger.info('Passed initial tests.');

    // set defaults on parameters and assign them to an object called qparam
    var qparam = getParams(req, res);
    logger.info('Parameters aquired.');

    getFields() // get a full list of fields requested
      .then(function (field_list) {

        qparam.field = addMarginOfErrorFields(field_list);
        logger.info('Field Parameter Set');

        var part1 = getFieldMeta(); // resolves to metadata info
        var part2 = getTableMeta(); // resolves to table metadata info
        var part3 = mainQuery(); // resolves to main query info

        Promise.all([part1, part2, part3])
          .then(function (success) {
            logger.info('Ready to assemble output.');
            // this is where you combine all the info together (meta from fields 
            // and tables, plus main query) into a final JS object to be returned
            var response = assembleOutput(success);
            logger.info('Assembled output. Returning');
            res.status(200).send(response);
          }).catch(catchError);

      }).catch(catchError);

    function catchError(reason) {
      logger.info('There has been an error: ' + reason);
      res.status(500).send(reason);
    }



    function addMarginOfErrorFields(field_list) {

      //break the comma delimited records from field into an array  
      var field_array = field_list.split(",");

      var moe_fields = [];

      //if moe is set to yes, add the moe version of each field (push into new array, then merge with existing)
      if (qparam.moe) {
        logger.info('Adding margin of error fields.');

        //if text _moe doesn't already occur in the field name. funny.  keeping for backwards compatibility.  allows _moe tables in '&table=' param
        field_array.forEach(function (d) {
          if (d.indexOf('_moe') === -1) {
            moe_fields.push(d.slice(0, -3) + '_moe' + d.slice(-3));
          }
        });

        // add in MOE fields
        field_array = field_array.concat(moe_fields);

      }

      //
      field_array = field_array.filter(onlyUnique);

      //send moe modified field list back to main field list
      // if there were no modifications, it was returned back the same (split/join canceled each other out)
      return field_array.join(',');

    }


    function assembleOutput(promise_output) {

      var column_metadata = promise_output[0];
      var table_metadata = promise_output[1];
      var query_data = promise_output[2];

      var data_array = query_data.map(function (d) {
        d.state = (d.state !== null) ? (d.state).toString() : null;
        d.place = (d.place !== null) ? (d.place).toString() : null;
        d.county = (d.county !== null) ? (d.county).toString() : null;
        return d;
      });


      if (qparam.type === 'csv') {
        logger.info('Customizing for type CSV.');

        //data_array to array, not object
        var notobject = [];
        var interarr = [];

        for (var q = 0; q < data_array.length; q++) {
          interarr = [];
          for (var key in data_array[q]) {
            if (data_array[q].hasOwnProperty(key)) {
              interarr.push(data_array[q][key]);
            }
          }
          notobject.push(interarr);
        }

        if (qparam.meta) {
          logger.info('Writing CSV metadata.');
          var metacsv = []; //array for csv field descriptions only

          //metacsv
          for (var n = 0; n < column_metadata.length; n++) {
            metacsv.push(column_metadata[n].column_title);
          }

          //add metadata to front (on second row)
          metacsv.unshift("Geographic Area Name", "State FIPS", "County FIPS", "Place FIPS", "Tract FIPS", "BG FIPS", "Unique ID");

          notobject.unshift(metacsv);
        }


        var ttlfields = qparam.field.split(",");

        //add column names to front
        ttlfields.unshift("geoname", "state", "county", "place", "tract", "bg", "geonum");
        notobject.unshift(ttlfields);

        res.setHeader('Content-disposition', 'attachment; filename=CO_DemogExport.csv');
        res.csv(notobject);

      }
      else {
        logger.info('Customizing for type JSON');

        // JSON Output

        var result = {};
        result.source = qparam.db;
        result.schema = qparam.schema;

        if (qparam.meta) {
          logger.info('Writing JSON metadata.');
          result.tablemeta = table_metadata;
          result.fieldmeta = column_metadata;
        }

        result.data = data_array;

        var pretty_print_json = qparam.pretty ? '  ' : '';

        res.set({
          "Content-Type": "application/json"
        });

        return (JSON.stringify(result, null, pretty_print_json));
      }


    }


    function mainQuery() {

      return new Promise(function (resolve, reject) {

        var joinlist = ""; //working string of 'where' condition to be inserted into sql query

        if (qparam.geonum) {
          //CASE 1:  you have a geonum.  essentially you don't care about anything else.  just get the data for that/those geonum(s)
          logger.info('CASE 1:  You have a geonum.  Just get the data for that/those geonum(s)');

          if (qparam.state || qparam.county || qparam.sumlev || qparam.geoid) {
            logger.warn('You specified either STATE, COUNTY, SUMLEV or GEOID.  These parameters are ignored when you also specify GEONUM');
          }

          joinlist = sqlList(qparam.geonum, 'geonum');
        }
        else if (qparam.geoid) {
          //CASE 2:  you have a geoid. just get the data for that/those geoid(s)
          logger.info('CASE 2: You have a geoid. just get the data for that/those geoid(s)');

          if (qparam.state || qparam.county || qparam.sumlev) {
            logger.warn('You specified either STATE, COUNTY or SUMLEV.  These parameters are ignored when you also specify GEOID');
          }

          var geonum_list = '1' + qparam.geoid.replace(/,/g, ',1'); // convert geoids to geonums
          joinlist = sqlList(geonum_list, "geonum");
        }
        else if (qparam.sumlev || qparam.county || qparam.state) {
          //CASE 3 - query
          logger.info('CASE 3: Query using one or all of: sumlev, county, or state.');
          var condition = getConditionString(qparam.sumlev, qparam.county, qparam.state);

          var countylist = qparam.county ? sqlList(qparam.county, 'county') : '';
          var statelist = qparam.state ? sqlList(qparam.state, 'state') : '';
          var sumlevlist = qparam.sumlev ? sqlList(qparam.sumlev, 'sumlev') : '';

          joinlist = constructWhereClause(condition, statelist, countylist, sumlevlist);

        }
        else {
          // CASE 4: No Geo
          logger.warn('No Geography in query.  Please specify either a geoname, geonum, sumlev, county, or state.');
          reject('No Geography in query.  Please specify either a geoname, geonum, sumlev, county, or state.');
        }

        var jointablelist = ""; //working string of tables to be inserted into sql query

        var table_array = getTableArray(qparam.field);

        //create a string to add to sql statement
        for (var n = 0; n < table_array.length; n++) {
          jointablelist = jointablelist + " natural join " + qparam.schema + "." + table_array[n];
        }

        //CONSTRUCT MAIN SQL STATEMENT
        // execute query
        var sql = "SELECT geoname, state, county, place, tract, bg, geonum, " + qparam.field + " from search." + qparam.schema + jointablelist + " where" + joinlist + " limit " + qparam.limit + ";";

        logger.info('Sending Main Query to the Database.');
        sendToDatabase(sql, conString, qparam.db).then(function (success) {
          logger.info('Main Query result successfully returned.');
          resolve(success);
        }, function (failure) {
          logger.info('Main Query failed.');
          reject(failure);
        });

      });
    }




    function getFields() {

      return new Promise(function (resolve, reject) {
        var field = qparam.field;
        var table = qparam.table;
        var schema = qparam.schema;
        var db = qparam.db;

        //if no fields are selected (then a table must be).  Create fields list based on the given table.
        if (!field) {
          logger.info('There are no fields in your query.  Retrieving a list of fields based upon your requested tables.');

          var table_list = sqlList(table, 'table_name');

          //Query table fields --ONLY SINGLE TABLE SELECT AT THIS TIME--
          var tablesql = "SELECT column_name from information_schema.columns where (" + table_list + ") and table_schema='" + schema + "';";

          logger.info('Sending request for a list of fields to the database.');
          var make_call_for_fields = sendToDatabase(tablesql, conString, db);

          make_call_for_fields.then(function (success) {

            if (success.length === 0) {
              logger.info('There are no valid field or table names in your request.');
              reject('There are no valid field or table names in your request.');
            }

            logger.info('Successfully returned a list of fields from the database.');
            var tcolumns = []; //columns gathered from table(s?)

            for (var i = 0; i < success.length; i++) {
              if (success[i].column_name !== 'geonum') {
                tcolumns.push(success[i].column_name);
              }
            }

            field = tcolumns.join(','); //$field becomes fields queried from info schema based upon table
            resolve(field);
          }, function (failure) {
            logger.info('Your database request for a list of fields has failed.');
            reject(failure);
          });


        }
        else {
          logger.info('You have specified fields in your query.  Tables will be ignored.');
          resolve(field);
        }

      }); // end new promise

    } // end get fields



    function getFieldMeta() {

      return new Promise(function (resolve, reject) {

        var field_array = qparam.field.split(",");
        var field_clause = sqlList(field_array, 'column_id');

        var field_metadata_sql = "SELECT column_id, column_verbose from " + qparam.schema + ".census_column_metadata where " + field_clause + ";";

        logger.info('Sending request for field metadata to the database.');

        sendToDatabase(field_metadata_sql, conString, qparam.db)
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
            logger.info('Your database request for field metadata has failed.');
            reject(failure);
          });

      }); // end new promise

    } // end getFieldMeta



    function getTableMeta() {

      return new Promise(function (resolve, reject) {

        var table_array = getTableArray(qparam.field);
        var table_clause = sqlList(table_array, 'table_id');

        var table_metadata_sql = "SELECT table_id, table_title, universe from " + qparam.schema + ".census_table_metadata where" + table_clause + ";";

        logger.info('Sending request for table metadata to the database.');

        sendToDatabase(table_metadata_sql, conString, qparam.db)
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
            logger.info('Your database request for table metadata has failed.');
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


function validateDB(db) {
  // validate database.  undefined is okay because it will be set to a default.

  var valid_db = [undefined, 'c1980', 'c1990', 'c2000', 'c2010', 'acs0812', 'acs0913', 'acs1014', 'acs1115'];

  if (valid_db.indexOf(db) !== -1) {
    logger.error(db + ' is not a valid database.  Alter validateDB function if this is in error.');
    return false;
  }
  else {
    return true;
  }
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

  //potential multi select (comma delimited list)
  obj.field = req.query.field;
  obj.state = req.query.state;
  obj.county = req.query.county;
  obj.geonum = req.query.geonum;
  obj.geoid = req.query.geoid;
  obj.sumlev = req.query.sumlev;
  obj.table = req.query.table;


  //potential single select
  obj.type = req.query.type || 'json';
  obj.db = req.query.db || 'acs1115';

  //set default for schema if it is missing
  obj.schema = req.query.schema || setDefaultSchema(obj.db, res);

  //by default limits to 1000 search results.  override by setting limit= in GET string
  obj.limit = parseInt(req.query.limit, 10) || 1000;

  obj.moe = getMOE(obj.db, req.query.moe);
  obj.pretty = getPretty(req.query.pretty);
  obj.meta = getMeta(req.query.meta);

  return obj;

}


function checkValidParams(query_params) {
  //get list of parameters - check all are valid

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

  if (condition === '001') {
    joinlist = " (" + statelist + ") ";
  }
  if (condition === '011') {
    joinlist = " (" + countylist + ") and (" + statelist + ") ";
  }
  if (condition === '111') {
    joinlist = " (" + sumlevlist + ") and (" + countylist + ") and (" + statelist + ") ";
  }
  if (condition === '010') {
    joinlist = " (" + countylist + ") ";
  }
  if (condition === '110') {
    joinlist = " (" + sumlevlist + ") and (" + countylist + ")";
  }
  if (condition === '100') {
    joinlist = " (" + sumlevlist + ") ";
  }
  if (condition === '101') {
    joinlist = " (" + sumlevlist + ") and (" + statelist + ") ";
  }

  return joinlist;
}


function getTableArray(fields) {
  //get a list of tables based upon characters in each field name  (convention: last 3 characters identify field number, previous characters are table name) 

  var field_list = fields.split(",");

  var table_list = [];

  field_list.forEach(function (d) {
    table_list.push(d.slice(0, -3));
  });

  //remove duplicate tables in array
  table_list = table_list.filter(onlyUnique);

  return table_list;
}



function immediateErrors(req, res) {
  // these warrant immediate warnings or early exits from the program

  if (req.query.field && req.query.table) {
    logger.warn('You specified TABLE.  This parameter is ignored when you also specify FIELD');
  }

  if (!validateDB(req.query.db)) {
    res.send('Database parameter ' + req.query.db + ' is not a valid database!');
    return;
  }

  //if no fields or tables are selected
  if (!req.query.table && !req.query.field) {
    res.send('You need to specify a table or fields to query.');
    return;
  }

}
