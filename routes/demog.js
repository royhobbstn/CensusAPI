//returns from: http://nodejs-server-royhobbstn.c9users.io/demog?db=acs1115&schema=data&table=b19013&moe=yes&geonum=108037,108039&type=json
/*
{"source":"acs1115","schema":"data","tablemeta":[{"table_id":"b19013","table_title":"MEDIAN HOUSEHOLD INCOME IN THE PAST 12 MONTHS (IN 2015 INFLATION-ADJUSTED DOLLARS)","universe":"Universe:  Households"}],"fieldmeta":[{"column_id":"b19013001","column_title":"Median household income in the past 12 months (in 2015 Inflation-adjusted dollars)"}],"data":[{"geoname":"Eagle County, Colorado","state":"8","county":"37","place":null,"tract":null,"bg":null,"geonum":"108037","b19013001":"72214","b19013_moe001":"3789"},{"geoname":"Elbert County, Colorado","state":"8","county":"39","place":null,"tract":null,"bg":null,"geonum":"108039","b19013001":"84963","b19013_moe001":"4050"}],"error":[]}
*/


// TODO pretty print option

var csv = require("express-csv");
var winston = require('winston');

var strpos = require("locutus/php/strings/strpos");
var substr_replace = require("locutus/php/strings/substr_replace");

var substr = require("locutus/php/strings/substr");
var implode = require("locutus/php/strings/implode");
var array_merge = require("locutus/php/array/array_merge");
var array_unshift = require("locutus/php/array/array_unshift");

module.exports = function (app, pg, conString) {


  app.get('/demog', function (req, res) {

    // notification of bad param in error output. 
    // TODO if !json, this error is swallowed.  not ideal.
    checkValidParams(req.query, errorarray);


    //potential multi select (comma delimited list)
    var field = req.query.field;
    var state = req.query.state;
    var county = req.query.county;
    var geonum = req.query.geonum;
    var geoid = req.query.geoid;
    var sumlev = req.query.sumlev;
    var table = req.query.table;


    //potential single select
    var type = req.query.type || 'json';
    var db = req.query.db || 'acs1115';

    if (!validateDB(db, errorarray)) {
      res.send('Database parameter ' + db + ' is not a valid database!');
      return;
    }

    //set default for schema if it is missing
    var schema = req.query.schema || setDefaultSchema(db, res);


    //by default limits to 1000 search results.  override by setting limit= in GET string
    var limit = parseInt(req.query.limit, 10) || 1000;

    var moe = getMOE(db, req.query.moe);

    //declare useful vars
    var fullarray = []; //final data array
    var metaarrfull = []; //final field metadata array
    var tblarrfull = []; //final table metadata array
    var errorarray = []; //store all errors and warnings

    //variables and arrays to use later
    var tablelist = []; //array of all tables used in query
    var jointablelist = ""; //working string of tables to be inserted into sql query
    var joinlist = ""; //working string of 'where' condition to be inserted into sql query

    var moefields = []; //moe field array
    var tcolumns = []; //columns gathered from table(s?)

    var metacsv = []; //array for csv field descriptions only
    var ttlfields = [];

    var lastbranchdone = 0;





    //if no fields or tables are selected
    if (!table && !field) {
      res.send('You need to specify a table or fields to query.');
      return;
    }

    //as far as errors go, tell people they are wasting their time specifying table(s) if they already specified field.
    if (field && table) {
      errorarray.push('You specified TABLE.  This parameter is ignored when you also specify FIELD');
    }


    //if no fields are selected (then a table must be).  Create fields list based on the given table.
    if (!field) {

      var table_list = sqlList(table, 'table_name');

      //Query table fields --ONLY SINGLE TABLE SELECT AT THIS TIME--
      var tablesql = "SELECT column_name from information_schema.columns where (" + table_list + ") and table_schema='" + schema + "';";

      field = sendinternal(tablesql, 1); //ASYNC

    }



    function check() {
      if (!field) {
        setTimeout(check, 50);
      }
      else {
        continue_program();
        main_logic();
      }
    }

    check();


    function continue_program() {

      function onlyUnique(value, index, self) {
        return self.indexOf(value) === index;
      }

      // we have fields: either hand entered or derived from tables

      //break the comma delimited records from field into an array  
      ttlfields = field.split(",");


      //if moe is set to yes, add the moe version of each field (push into new array, then merge with existing)
      if (moe) {

        var pos;

        for (var k = 0; k < ttlfields.length; k++) {
          //if text _moe doesn't already occur in the field name
          pos = strpos(ttlfields[k], '_moe');
          if (pos === false) {
            moefields.push(substr_replace(ttlfields[k], '_moe', -3, 0));
          }
        }

        ttlfields = array_merge(ttlfields, moefields);

        //remove duplicate field names



        ttlfields = ttlfields.filter(onlyUnique);


        //send moe modified field list back to main field list
        field = implode(',', ttlfields);
      }



      //get a list of tables based upon characters in each field name  (convention: last 3 characters identify field number, previous characters are table name) 
      for (var m = 0; m < ttlfields.length; m++) {
        tablelist.push(substr(ttlfields[m], 0, -3));
      }

      //remove duplicate tables in array
      tablelist = tablelist.filter(onlyUnique);

      //create a string to add to sql statement
      for (var n = 0; n < tablelist.length; n++) {
        jointablelist = jointablelist + " natural join " + schema + "." + tablelist[n];
      }

      //validate all fields exist

      //validate geoid

      //validate geonum



      //this is where field metadata is gathered
      var metafieldlist = "";
      //construct 'where' statement for column_id metadata
      //iterate through all fields
      for (var p = 0; p < ttlfields.length; p++) {
        metafieldlist = metafieldlist + " column_id='" + ttlfields[p] + "' or";
      }

      //trim last trailing 'or'
      metafieldlist = substr(metafieldlist, 0, -2);


      //Query metadata
      var metasql = "SELECT column_id, column_verbose from " + schema + ".census_column_metadata where " + metafieldlist + ";";

      sendinternal(metasql, 2); //ASYNC



    }


    function main_logic() {

      var joinlist;

      //CASE 1:  you have a geonum
      //essentially you don't care about anything else.  just get the data for that/those geonum(s)
      if (geonum) {

        //as far as errors go, tell people they are wasting their time specifying sumlev, state, county, place and geoid if they specified geonum.
        if (state) {
          errorarray.push('You specified STATE.  This parameter is ignored when you also specify GEONUM');
        }
        if (county) {
          errorarray.push('You specified COUNTY.  This parameter is ignored when you also specify GEONUM');
        }
        if (sumlev) {
          errorarray.push('You specified SUMLEV.  This parameter is ignored when you also specify GEONUM');
        }
        if (geoid) {
          errorarray.push('You specified GEOID.  This parameter is ignored when you also specify GEONUM');
        }

        joinlist = sqlList(geonum, 'geonum');

        //END CASE 1
      }
      else if (geoid) {
        //CASE 2:  you have a geoid

        //as far as errors go, tell people they are wasting their time specifying sumlev, state, county, place and geoid if they specified geonum.
        if (state) {
          errorarray.push('You specified STATE.  This parameter is ignored when you also specify GEOID');
        }
        if (county) {
          errorarray.push('You specified COUNTY.  This parameter is ignored when you also specify GEOID');
        }
        if (sumlev) {
          errorarray.push('You specified SUMLEV.  This parameter is ignored when you also specify GEOID');
        }


        joinlist = sqlListGeoID(geoid, "geonum");

        console.log(joinlist);

        //END CASE 2  
      }
      else if (sumlev || county || state) {
        //CASE 3 - query

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


        if (county) {
          //create county array out of delimited list
          var countylist = sqlList(county, 'county');
        }


        if (state) {
          //create state array out of delimited list
          var statelist = sqlList(state, 'state');
        }

        // TODO warning sumlev is STATE 40 but county is given

        if (sumlev) {
          //create sumlev array out of delimited list
          var sumlevlist = sqlList(sumlev, 'sumlev');
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
        errorarray.push('No geography specified.');
        return 'error'; //goto a;
        //END CASE 4
      }


      //CONSTRUCT MAIN SQL STATEMENT
      // execute query
      var sql = "SELECT geoname, state, county, place, tract, bg, geonum, " + field + " from search." + schema + jointablelist + " where" + joinlist + " limit " + limit + ";";

      //console.log(sql);

      sendtodatabase(sql); //ASYNC

    }

    function formatandsend() {

      function check2() {
        if (lastbranchdone === 0) {
          //console.log('waiting tablemeta...');
          setTimeout(check2, 50);
        }
        else {
          createlast();
        }
      }

      check2();


      function createlast() {
        //meta first record is combined with results from iteration over every query result row
        var withmeta = {};
        withmeta.source = db;
        withmeta.schema = schema;
        withmeta.tablemeta = tblarrfull;
        withmeta.fieldmeta = metaarrfull;
        withmeta.data = fullarray;
        withmeta.error = errorarray;


        if (type === 'csv') {

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
          array_unshift(ttlfields, "geonum");
          array_unshift(ttlfields, "bg");
          array_unshift(ttlfields, "tract");
          array_unshift(ttlfields, "place");
          array_unshift(ttlfields, "county");
          array_unshift(ttlfields, "state");
          array_unshift(ttlfields, "geoname");

          //add geonum description to front of metadata row array
          array_unshift(metacsv, "Unique ID");
          array_unshift(metacsv, "BG FIPS");
          array_unshift(metacsv, "Tract FIPS");
          array_unshift(metacsv, "Place FIPS");
          array_unshift(metacsv, "County FIPS");
          array_unshift(metacsv, "State FIPS");
          array_unshift(metacsv, "Geographic Area Name");

          array_unshift(notobject, metacsv);
          array_unshift(notobject, ttlfields);

          //console.log(notobject);
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

      return;
    }

    return;




    function sendtodatabase(sqlstring) {

      console.log(sqlstring);
      var client = new pg.Client(conString + db);
      console.log("a:", conString + db);

      client.connect(function (err) {

        if (err) {
          return console.error('could not connect to postgres', err);
        }

        client.query(sqlstring, function (err, result) {

          if (err) {
            return console.error('error running query', err);
          }

          var resultdata = result.rows;


          //add geoname as first element in every result record array
          fullarray = [];
          var tempobject = {};

          for (var t = 0; t < resultdata.length; t++) {
            //console.log(resultdata[t]);

            tempobject = {};

            for (var key in resultdata[t]) {
              if (resultdata[t].hasOwnProperty(key)) {
                tempobject[key] = resultdata[t][key];
                if ((key === 'state' || key === 'place' || key === 'county') && (resultdata[t][key])) {
                  tempobject[key] = (resultdata[t][key]).toString(); //the three vars above need to be converted to strings to be consistant. (if not null)
                }
              }
            }

            fullarray.push(tempobject);

          }


          formatandsend();


          client.end();

        });
      });
    }

    function sendinternal(sqlstring, branch) {

      console.log(sqlstring);
      var client = new pg.Client(conString + db);
      console.log("b:", conString + db);

      client.connect(function (err) {

        if (err) {
          return console.error('could not connect to postgres', err);
        }

        client.query(sqlstring, function (err, result) {

          if (err) {
            return console.error('error running query', err);
          }

          client.end();

          var tableresult = (result.rows);


          if (branch === 1) {
            for (var i = 0; i < tableresult.length; i++) {
              if (tableresult[i].column_name !== 'geonum') {
                tcolumns.push(tableresult[i].column_name);
              }
            }

            field = implode(',', tcolumns); //$field becomes fields queried from info schema based upon table

            return field;
          } //end branch 1

          if (branch === 2) {

            var metaarr = {};

            for (var k = 0; k < tableresult.length; k++) {

              metaarr = {};
              metaarr.column_id = tableresult[k].column_id;
              metaarr.column_title = tableresult[k].column_verbose;

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
            var tblstr = "";
            //construct 'where' statement for column_id metadata
            //iterate through all fields
            for (var p = 0; p < tablelist.length; p++) {
              tblstr = tblstr + " table_id='" + tablelist[p] + "' or";
            }

            //trim last trailing 'or'
            tblstr = substr(tblstr, 0, -2);


            //Query metadata
            var tblsql = "SELECT table_id, table_title, universe from " + schema + ".census_table_metadata where" + tblstr + ";";

            sendinternal(tblsql, 3); //ASYNC

          } //end branch2

          if (branch === 3) {
            var tblarr = {};
            for (var q = 0; q < tableresult.length; q++) {
              tblarr = {};
              tblarr.table_id = tableresult[q].table_id;
              tblarr.table_title = tableresult[q].table_title;
              tblarr.universe = tableresult[q].universe;
              tblarrfull.push(tblarr);
            }
            lastbranchdone = 1;
            branch = 0; //just in case
            return;

          } //end branch 3

          return;

        });
      });
    }

  });

};





function checkValidParams(query, error) {

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
      error.push('Your parameter -' + getkey[i] + '- is not valid.');
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

  var list_array = str_list.split(",");

  var str = "";

  list_array.forEach(function (d) {
    str = str + " " + field_name + "='" + d + "' or";
  });

  console.log(str.slice(0, -2));
  //trim last trailing 'or'
  return str.slice(0, -2);

}


function sqlListGeoID(str_list, field_name) {

  var list_array = str_list.split(",");

  var str = "";

  list_array.forEach(function (d) {
    str = str + " " + field_name + "='1" + d + "' or";
  });

  console.log(str.slice(0, -2));
  //trim last trailing 'or'
  return str.slice(0, -2);

}


function validateDB(db, error) {
  //validate database
  if (db !== 'c1980' && db !== 'c1990' && db !== 'c2000' && db !== 'c2010' && db !== 'acs0812' && db !== 'acs0913' && db !== 'acs1014' && db !== 'acs1115') {
    error.push('Your database choice `' + db + '` is not valid.');
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
  // TODO, just check first three letters of db
  if (db === 'acs0812' || db === 'acs0913' || db === 'acs1014' || db === 'acs1115') {
    if (moe === 'yes' || moe === 'y' || moe === 'true') {
      return true;
    }
  }
  return false;
}
