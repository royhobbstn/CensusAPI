//returns from: http://nodejs-server-royhobbstn.c9users.io/demog?db=acs1115&schema=data&table=b19013&moe=yes&geonum=108037,108039&type=json
/*
{"source":"acs1115","schema":"data","tablemeta":[{"table_id":"b19013","table_title":"MEDIAN HOUSEHOLD INCOME IN THE PAST 12 MONTHS (IN 2015 INFLATION-ADJUSTED DOLLARS)","universe":"Universe:  Households"}],"fieldmeta":[{"column_id":"b19013001","column_title":"Median household income in the past 12 months (in 2015 Inflation-adjusted dollars)"}],"data":[{"geoname":"Eagle County, Colorado","state":"8","county":"37","place":null,"tract":null,"bg":null,"geonum":"108037","b19013001":"72214","b19013_moe001":"3789"},{"geoname":"Elbert County, Colorado","state":"8","county":"39","place":null,"tract":null,"bg":null,"geonum":"108039","b19013001":"84963","b19013_moe001":"4050"}],"error":[]}
*/

// TODO RACE condition with Table and Field Meta Data
// TODO pretty print option
// TODO test coverage

var csv = require("express-csv");
var winston = require('winston');
var pg = require('pg');

module.exports = function (app, pg, conString) {


  app.get('/demog', function (req, res) {

    // if the query is bad, make a quick exit
    immediateErrors(req, res);

    // set defaults on parameters and assign them to an object called qparam
    var qparam = getParams(req, res);


    // get Fields

    // get Field Meta

    // get Table Meta

    // main Query



    // populate fields if only table was given
    var wait_for_fields = getFields(qparam.field, qparam.table, qparam.schema);

    wait_for_fields.then(function (success) {
      qparam.field = success;
      continue_program();
      main_logic();
    }, function (failure) {
      console.log(failure);
    });


    //declare useful vars
    var fullarray = []; //final data array
    var metaarrfull = []; //final field metadata array
    var tblarrfull = []; //final table metadata array

    //variables and arrays to use later
    var tablelist = []; //array of all tables used in query
    var jointablelist = ""; //working string of tables to be inserted into sql query

    var moefields = []; //moe field array


    var metacsv = []; //array for csv field descriptions only
    var ttlfields = [];

    function continue_program() {


      // we have fields: either hand entered or derived from tables

      //break the comma delimited records from field into an array  
      ttlfields = qparam.field.split(",");


      //if moe is set to yes, add the moe version of each field (push into new array, then merge with existing)
      if (qparam.moe) {

        //if text _moe doesn't already occur in the field name. funny.  keeping for backwards compatibility.  allows _moe tables in '&table=' param
        ttlfields.forEach(function (d) {
          if (d.indexOf('_moe') === -1) {
            moefields.push(d.slice(0, -3) + '_moe' + d.slice(-3));
          }
        });


        // add in MOE fields
        ttlfields = ttlfields.concat(moefields);

        ttlfields = ttlfields.filter(onlyUnique);

        //send moe modified field list back to main field list
        qparam.field = ttlfields.join(',');

      }



      //get a list of tables based upon characters in each field name  (convention: last 3 characters identify field number, previous characters are table name) 
      ttlfields.forEach(function (d) {
        tablelist.push(d.slice(0, -3));
      });
      // TODO the above does not support moe fields in field list. does it?

      //remove duplicate tables in array
      tablelist = tablelist.filter(onlyUnique);

      //create a string to add to sql statement
      for (var n = 0; n < tablelist.length; n++) {
        jointablelist = jointablelist + " natural join " + qparam.schema + "." + tablelist[n];
      }

      //validate all fields exist

      //validate geoid

      //validate geonum

      //this is where field metadata is gathered
      var metafieldlist = sqlList(ttlfields, 'column_id');

      //Query metadata
      var metasql = "SELECT column_id, column_verbose from " + qparam.schema + ".census_column_metadata where " + metafieldlist + ";";

      var second_request = sendToDatabase(metasql, conString, qparam.db); //ASYNC

      second_request.then(function (success) {
        var metaarr = {};

        for (var k = 0; k < success.length; k++) {

          metaarr = {};
          metaarr.column_id = success[k].column_id;
          metaarr.column_title = success[k].column_verbose;

          metaarrfull.push(metaarr);


        } //end k

        //metacsv
        for (var m = 0; m < ttlfields.length; m++) {
          for (var n = 0; n < metaarrfull.length; n++) {
            if (ttlfields[m] === metaarrfull[n].column_id) {
              metacsv.push(metaarrfull[n].column_title);
            }
          }
        }


        //this is where table metadata is gathered
        var tblstr = sqlList(tablelist, 'table_id');

        //Query metadata
        var tblsql = "SELECT table_id, table_title, universe from " + qparam.schema + ".census_table_metadata where" + tblstr + ";";

        var third_request = sendToDatabase(tblsql, conString, qparam.db); //ASYNC

        third_request.then(function (tableresult) {
          var tblarr = {};
          for (var q = 0; q < tableresult.length; q++) {
            tblarr = {};
            tblarr.table_id = tableresult[q].table_id;
            tblarr.table_title = tableresult[q].table_title;
            tblarr.universe = tableresult[q].universe;
            tblarrfull.push(tblarr);
          }


        });

      });

    }


    function main_logic() {

      var joinlist = ""; //working string of 'where' condition to be inserted into sql query


      //CASE 1:  you have a geonum.  essentially you don't care about anything else.  just get the data for that/those geonum(s)
      if (qparam.geonum) {

        ignoreGeonum(qparam.state, qparam.county, qparam.sumlev, qparam.geoid);

        joinlist = sqlList(qparam.geonum, 'geonum');

        //END CASE 1
      }
      else if (qparam.geoid) {
        //CASE 2:  you have a geoid. just get the data for that/those geoid(s)

        ignoreGeoid(qparam.state, qparam.county, qparam.sumlev);




        joinlist = sqlListGeoID(qparam.geoid, "geonum");


        //END CASE 2  
      }
      else if (qparam.sumlev || qparam.county || qparam.state) {
        //CASE 3 - query

        var condition = getConditionString(qparam.sumlev, qparam.county, qparam.state);


        if (qparam.county) {
          //create county array out of delimited list
          var countylist = sqlList(qparam.county, 'county');
        }


        if (qparam.state) {
          //create state array out of delimited list
          var statelist = sqlList(qparam.state, 'state');
        }

        // TODO warning sumlev is STATE 40 but county is given

        if (qparam.sumlev) {
          //create sumlev array out of delimited list
          var sumlevlist = sqlList(qparam.sumlev, 'sumlev');
        }

        //every possible combination of sumlev, county, state
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

        //END CASE 3
      }
      else {
        // CASE 4: No Geo
        winston.warn('No geography specified.');
        return 'error';
        //END CASE 4
      }


      //CONSTRUCT MAIN SQL STATEMENT
      // execute query
      var sql = "SELECT geoname, state, county, place, tract, bg, geonum, " + qparam.field + " from search." + qparam.schema + jointablelist + " where" + joinlist + " limit " + qparam.limit + ";";


      var final_call = sendToDatabase(sql, conString, qparam.db);

      final_call.then(function (success) {
        //add geoname as first element in every result record array
        fullarray = [];
        var tempobject = {};

        for (var t = 0; t < success.length; t++) {

          tempobject = {};

          for (var key in success[t]) {
            if (success[t].hasOwnProperty(key)) {
              tempobject[key] = success[t][key];
              if ((key === 'state' || key === 'place' || key === 'county') && (success[t][key])) {
                tempobject[key] = (success[t][key]).toString(); //the three vars above need to be converted to strings to be consistant. (if not null)
              }
            }
          }

          fullarray.push(tempobject);

        }

        createlast();

      }, function (failure) {
        console.log(failure);
      });

    }


    function createlast() {
      //meta first record is combined with results from iteration over every query result row
      var withmeta = {};
      withmeta.source = qparam.db;
      withmeta.schema = qparam.schema;
      withmeta.tablemeta = tblarrfull;
      withmeta.fieldmeta = metaarrfull;
      withmeta.data = fullarray;

      if (qparam.type === 'csv') {

        //fullarray to array, not object
        var notobject = [];
        var interarr = [];

        for (var q = 0; q < fullarray.length; q++) {
          interarr = [];
          for (var key in fullarray[q]) {
            if (fullarray[q].hasOwnProperty(key)) {
              interarr.push(fullarray[q][key]);
            }
          }
          notobject.push(interarr);
        }

        //add geonum to front of fields row array
        ttlfields.unshift("geoname", "state", "county", "place", "tract", "bg", "geonum");

        //add geonum description to front of metadata row array
        metacsv.unshift("Geographic Area Name", "State FIPS", "County FIPS", "Place FIPS", "Tract FIPS", "BG FIPS", "Unique ID");

        notobject.unshift(metacsv, ttlfields);

        res.setHeader('Content-disposition', 'attachment; filename=CO_DemogExport.csv');
        res.csv(notobject);

      }
      else {

        res.set({
          "Content-Type": "application/json"
        });
        res.send(JSON.stringify(withmeta));
      }



      return;
    }



    function getFields(field, table, schema) {
      //if no fields are selected (then a table must be).  Create fields list based on the given table.
      if (!field) {
        return new Promise(function (resolve, reject) {
          var table_list = sqlList(table, 'table_name');

          //Query table fields --ONLY SINGLE TABLE SELECT AT THIS TIME--
          var tablesql = "SELECT column_name from information_schema.columns where (" + table_list + ") and table_schema='" + schema + "';";

          var make_call_for_fields = sendToDatabase(tablesql, conString, qparam.db);

          make_call_for_fields.then(function (success) {

            var tcolumns = []; //columns gathered from table(s?)

            for (var i = 0; i < success.length; i++) {
              if (success[i].column_name !== 'geonum') {
                tcolumns.push(success[i].column_name);
              }
            }

            field = tcolumns.join(','); //$field becomes fields queried from info schema based upon table
            resolve(field);
          }, function (failure) {
            console.log('this is bad');
            reject(failure);
          });

        });
      }
      else {
        return new Promise(function (resolve) {
          resolve(field);
        });
      }
    }





  });

};





function checkValidParams(query) {

  //get list of all parameters - check all are valid
  var getkey = [];

  //push $_POST vars into simple array for each $key
  for (var propName in query) {
    if (query.hasOwnProperty(propName)) {
      getkey.push(propName);
    }
  }

  //loop through $keys, check against list of valid values
  for (var i = 0; i < getkey.length; i++) {
    if (getkey[i] !== 'field' && getkey[i] !== 'state' && getkey[i] !== 'county' && getkey[i] !== 'sumlev' && getkey[i] !== 'place' && getkey[i] !== 'geonum' && getkey[i] !== 'geoid' && getkey[i] !== 'db' && getkey[i] !== 'schema' && getkey[i] !== 'geo' && getkey[i] !== 'series' && getkey[i] !== 'type' && getkey[i] !== 'limit' && getkey[i] !== 'moe' && getkey[i] !== 'table') {
      winston.warn('Your parameter -' + getkey[i] + '- is not valid.');
    }
  }

}



function makeRequest(method, url) {
  return new Promise(function (resolve, reject) {
    var xhr = new XMLHttpRequest();
    xhr.open(method, url);
    xhr.onload = function () {
      if (this.status >= 200 && this.status < 300) {
        resolve(xhr.response);
      }
      else {
        reject({
          status: this.status,
          statusText: xhr.statusText
        });
      }
    };
    xhr.onerror = function () {
      reject({
        status: this.status,
        statusText: xhr.statusText
      });
    };
    xhr.send();
  });
}



function sqlList(str_list, field_name) {

  var list_array = [];

  // will work with arrays or comma delimited strings
  if (!Array.isArray(str_list)) {
    list_array = str_list.split(",");
  }
  else {
    list_array = str_list;
  }

  var str = "";

  list_array.forEach(function (d) {
    str = str + " " + field_name + "='" + d + "' or";
  });

  return str.slice(0, -2);

}

// hrm.  similar to above.  refactor possible without being too abstract?
function sqlListGeoID(str_list, field_name) {

  var list_array = [];

  // will work with arrays or comma delimited strings
  if (!Array.isArray(str_list)) {
    list_array = str_list.split(",");
  }
  else {
    list_array = str_list;
  }

  var str = "";

  list_array.forEach(function (d) {
    str = str + " " + field_name + "='1" + d + "' or";
  });

  return str.slice(0, -2);

}


function validateDB(db) {
  // validate database.  undefined is okay because it will be set to a default.
  if (db !== undefined && db !== 'c1980' && db !== 'c1990' && db !== 'c2000' && db !== 'c2010' && db !== 'acs0812' && db !== 'acs0913' && db !== 'acs1014' && db !== 'acs1115') {
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
  //if database is acs, check to see if moe option is flagged
  if (db.slice(0, 3) === 'acs') {
    if (moe === 'yes' || moe === 'y' || moe === 'true') {
      return true;
    }
  }
  return false;
}


// how does this work??
function onlyUnique(value, index, self) {
  return self.indexOf(value) === index;
}


function getConditionString(sumlev, county, state) {
  var condition = ""; //condition is going to be a 3 character string which identifies sumlev, county, state (yes/no) (1,0)

  if (sumlev) {
    condition = "1";
  }
  else {
    condition = "0";
  }

  if (county) {
    condition = condition + "1";
  }
  else {
    condition = condition + "0";
  }

  if (state) {
    condition = condition + "1";
  }
  else {
    condition = condition + "0";
  }

  return condition;
}



function ignoreGeonum(state, county, sumlev, geoid) {
  //as far as errors go, tell people they are wasting their time specifying sumlev, state, county and geoid if they specified geonum.
  if (state) {
    winston.warn('You specified STATE.  This parameter is ignored when you also specify GEONUM');
  }
  if (county) {
    winston.warn('You specified COUNTY.  This parameter is ignored when you also specify GEONUM');
  }
  if (sumlev) {
    winston.warn('You specified SUMLEV.  This parameter is ignored when you also specify GEONUM');
  }
  if (geoid) {
    winston.warn('You specified GEOID.  This parameter is ignored when you also specify GEONUM');
  }
}


function ignoreGeoid(state, county, sumlev) {
  //as far as errors go, tell people they are wasting their time specifying sumlev, state, and county if they specified geoid.
  if (state) {
    winston.warn('You specified STATE.  This parameter is ignored when you also specify GEOID');
  }
  if (county) {
    winston.warn('You specified COUNTY.  This parameter is ignored when you also specify GEOID');
  }
  if (sumlev) {
    winston.warn('You specified SUMLEV.  This parameter is ignored when you also specify GEOID');
  }

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

  return obj;

}


function immediateErrors(req, res) {
  // these warrant immediate warnings or early exits from the program

  if (req.query.field && req.query.table) {
    winston.warn('You specified TABLE.  This parameter is ignored when you also specify FIELD');
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



function sendToDatabase(sqlstring, conString, db) {
  winston.info(sqlstring);
  return new Promise(function (resolve, reject) {
    var client = new pg.Client(conString + db);
    client.connect(function (err) {
      if (err) {
        client.end();
        reject('could not connect');
      }
      client.query(sqlstring, function (err, result) {
        if (err) {
          client.end();
          reject('bad query');
        }
        var tableresult = (result.rows);
        client.end();
        resolve(tableresult);
      });
    });

  });
}
