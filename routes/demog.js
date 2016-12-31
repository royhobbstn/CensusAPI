//MICROSERVICE for getting data triggered by an advanced query

//returns from: http://nodejs-server-royhobbstn.c9users.io/demog?db=acs1115&schema=data&table=b19013&moe=yes&geonum=108037,108039&type=json
/*
{"source":"acs1115","schema":"data","tablemeta":[{"table_id":"b19013","table_title":"MEDIAN HOUSEHOLD INCOME IN THE PAST 12 MONTHS (IN 2015 INFLATION-ADJUSTED DOLLARS)","universe":"Universe:  Households"}],"fieldmeta":[{"column_id":"b19013001","column_title":"Median household income in the past 12 months (in 2015 Inflation-adjusted dollars)"}],"data":[{"geoname":"Eagle County, Colorado","state":"8","county":"37","place":null,"tract":null,"bg":null,"geonum":"108037","b19013001":"72214","b19013_moe001":"3789"},{"geoname":"Elbert County, Colorado","state":"8","county":"39","place":null,"tract":null,"bg":null,"geonum":"108039","b19013001":"84963","b19013_moe001":"4050"}],"error":[]}
*/

var csv = require("express-csv");

var strpos = require("locutus/php/strings/strpos");
var substr_replace = require("locutus/php/strings/substr_replace");
var explode = require("locutus/php/strings/explode");
var substr = require("locutus/php/strings/substr");
var implode = require("locutus/php/strings/implode");
var array_merge = require("locutus/php/array/array_merge");
var array_unshift = require("locutus/php/array/array_unshift");

module.exports = function (app, pg, conString) {


  app.get('/demog', function (req, res) {

    //potential multi select (comma delimited list)
    var field = req.query.field || "undefined";
    var state = req.query.state || "undefined";
    var county = req.query.county || "undefined";
    var geonum = req.query.geonum || "undefined";
    var geoid = req.query.geoid || "undefined";
    var sumlev = req.query.sumlev || "undefined";
    var table = req.query.table || "undefined";


    //potential single select
    var type = req.query.type || 'json';
    var db = req.query.db || 'acs1115';
    //set default for schema if it is missing
    var schema = req.query.schema || function () {
      if (db === 'acs1115' || db === 'acs1014' || db === 'acs0913' || db === 'acs0812' || db === 'c2010') {
        return 'data';
      }
      if (db === 'c2000' || db === 'c1990' || db === 'c1980') {
        return 'sf1';
      }
      return ''; //no valid database - will deal with later 
    }();

    conString = "postgres://codemog:demography@acs-export.cudcner3avpd.us-west-2.rds.amazonaws.com:5432/" + db;
    var geo = req.query.geo || "undefined";
    var series = req.query.series || "undefined";
    var type = req.query.type || "undefined";
    //by default limits to 1000 search results.  override by setting limit= in GET string
    var limit = parseInt(req.query.limit, 10) || 1000;

    //if database is acs, check to see if moe option is flagged
    var moe = 'no';
    if (db === 'acs0812' || db === 'acs0913' || db === 'acs1014' || db === 'acs1115') {
      if (req.query.moe) {
        moe = req.query.moe;
      }
    }


    //declare useful vars
    var fullarray = []; //final data array
    var metaarrfull = []; //final field metadata array
    var tblarrfull = []; //final table metadata array
    var errorarray = []; //store all errors and warnings

    //variables and arrays to use later
    var tablelist = []; //array of all tables used in query
    var jointablelist = ""; //working string of tables to be inserted into sql query
    var joinlist = ""; //working string of 'where' condition to be inserted into sql query
    var arr = [];
    var arr2 = [];
    var moefields = []; //moe field array
    var tcolumns = []; //columns gathered from table(s?)

    //get list of all parameters - check all are valid
    var getkey = [];

    //<>
    var metacsv = []; //array for csv field descriptions only
    //<>
    var ttlfields = [];

    var lastbranchdone = 0;


    //push $_POST vars into simple array for each $key

    //array_push($getkey,$key);  

    for (var propName in req.query) {
      if (req.query.hasOwnProperty(propName)) {
        getkey.push(propName);
      }
    }


    //loop through $keys, check against list of valid values
    for (var i = 0; i < getkey.length; i++) {
      if (getkey[i] !== 'field' && getkey[i] !== 'state' && getkey[i] !== 'county' && getkey[i] !== 'sumlev' && getkey[i] !== 'place' && getkey[i] !== 'geonum' && getkey[i] !== 'geoid' && getkey[i] !== 'db' && getkey[i] !== 'schema' && getkey[i] !== 'geo' && getkey[i] !== 'series' && getkey[i] !== 'type' && getkey[i] !== 'limit' && getkey[i] !== 'moe' && getkey[i] !== 'table' && getkey[i] !== 'type') {
        errorarray.push('Your parameter -' + getkey[i] + '- is not valid.');
      }
    }

    //validate database selected
    if (db !== 'c1980' && db !== 'c1990' && db !== 'c2000' && db !== 'c2010' && db !== 'acs0812' && db !== 'acs0913' && db !== 'acs1014' && db !== 'acs1115') {
      errorarray.push('Your database choice `' + db + '` is not valid.');
      db = "";
      return 'error'; //goto a;
    }


    //if no fields or tables are selected
    if (table === "undefined" && field === "undefined") {
      errorarray.push('You need to specify a table or fields to query.');
      return 'error'; //goto a;
    }

    //as far as errors go, tell people they are wasting their time specifying table(s) if they specified field.
    if (field !== "undefined" && table !== "undefined") {
      errorarray.push('You specified TABLE.  This parameter is ignored when you also specify FIELD');
    }


    //if no fields are selected (then a table must be).  Create fields list based on the given table.
    if (field === "undefined") {

      var atablearray = explode(",", table);


      var atablestr = "";

      for (var j = 0; j < atablearray.length; j++) {
        atablestr = atablestr + " table_name='" + atablearray[j] + "' or";
      }

      //trim last trailing 'or'
      atablestr = substr(atablestr, 0, -2);

      //Query table fields --ONLY SINGLE TABLE SELECT AT THIS TIME--
      var tablesql = "SELECT column_name from information_schema.columns where (" + atablestr + ") and table_schema='" + schema + "';";

      field = sendinternal(tablesql, 1); //ASYNC

    }


    function check() {
      if (field === undefined || field === "undefined") {
        setTimeout(check, 50);
        //console.log('waiting...');
      }
      else {
        //console.log('field: '+field);
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
      ttlfields = explode(",", field);


      //if moe is set to yes, add the moe version of each field (push into new array, then merge with existing)
      if (moe === 'yes') {

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
      //CASE 1:  you have a geonum
      //essentially you don't care about anything else.  just get the data for that/those geonum(s)
      if (geonum !== "undefined") {

        //as far as errors go, tell people they are wasting their time specifying sumlev, state, county, place and geoid if they specified geonum.
        if (state !== "undefined") {
          errorarray.push('You specified STATE.  This parameter is ignored when you also specify GEONUM');
        };
        if (county !== "undefined") {
          errorarray.push('You specified COUNTY.  This parameter is ignored when you also specify GEONUM');
        };
        if (sumlev !== "undefined") {
          errorarray.push('You specified SUMLEV.  This parameter is ignored when you also specify GEONUM');
        };
        if (geoid !== "undefined") {
          errorarray.push('You specified GEOID.  This parameter is ignored when you also specify GEONUM');
        };

        //break the comma delimited records from geonum into an array  
        var geonumarray = explode(",", geonum);

        //iterate through all geonum's
        for (var z = 0; z < geonumarray.length; z++) {
          joinlist = joinlist + " geonum=" + geonumarray[z] + " or";
        }


        //trim last trailing 'or'
        joinlist = substr(joinlist, 0, -2);

        //END CASE 1
      }
      else if (geoid !== "undefined") {
        //CASE 2:  you have a geoid

        //as far as errors go, tell people they are wasting their time specifying sumlev, state, county, place and geoid if they specified geonum.
        if (state !== "undefined") {
          errorarray.push('You specified STATE.  This parameter is ignored when you also specify GEOID');
        };
        if (county !== "undefined") {
          errorarray.push('You specified COUNTY.  This parameter is ignored when you also specify GEOID');
        };
        if (sumlev !== "undefined") {
          errorarray.push('You specified SUMLEV.  This parameter is ignored when you also specify GEOID');
        };

        //break the comma delimited records from geonum into an array  
        var geoidarray = explode(",", geoid);

        //iterate through all geoids, simply put a '1' in front and treat them like geonums
        for (var y = 0; y < geoidarray.length; y++) {
          joinlist = joinlist + " geonum=1" + geoidarray[y] + " or";
        }
        //trim last trailing 'or'
        joinlist = substr(joinlist, 0, -2);

        //END CASE 2  
      }
      else if (sumlev !== "undefined" || county !== "undefined" || state !== "undefined") {
        //CASE 3 - query

        var condition = ""; //condition is going to be a 3 character string which identifies sumlev, county, state (yes/no) (1,0)
        if (sumlev !== "undefined") {
          condition = "1";
        }
        else {
          condition = "0";
        }
        if (county !== "undefined") {
          condition = condition + "1";
        }
        else {
          condition = condition + "0";
        }
        if (state !== "undefined") {
          condition = condition + "1";
        }
        else {
          condition = condition + "0";
        }



        if (county !== "undefined") {
          //create county array out of delimited list
          var countylist = "";

          //break the comma delimited records from county into an array  
          var countyarray = explode(",", county);

          //iterate through all counties
          for (var x = 0; x < countyarray.length; x++) {
            countylist = countylist + " county=" + countyarray[x] + " or";
          }

          //trim last trailing 'or'
          countylist = substr(countylist, 0, -2);
        }


        if (state !== "undefined") {
          //create state array out of delimited list
          var statelist = "";

          //break the comma delimited records from county into an array  
          var statearray = explode(",", state);

          //iterate through all states
          for (var u = 0; u < statearray.length; u++) {
            statelist = statelist + " state=" + statearray[u] + " or";
          }

          //trim last trailing 'or'
          statelist = substr(statelist, 0, -2);
        }


        if (sumlev !== "undefined") {
          //create sumlev array out of delimited list
          var sumlevlist = "";

          //break the comma delimited records from county into an array  
          var sumlevarray = explode(",", sumlev);

          //iterate through all sumlevs
          for (var v = 0; v < sumlevarray.length; v++) {
            sumlevlist = sumlevlist + " sumlev=" + sumlevarray[v] + " or";
          }

          //trim last trailing 'or'
          sumlevlist = substr(sumlevlist, 0, -2);
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

          //header("Content-Type: text/csv");
          //header("Content-Disposition: attachment; filename=file.csv");
          // Disable caching
          //header("Cache-Control: no-cache, no-store, must-revalidate"); // HTTP 1.1
          //header("Pragma: no-cache"); // HTTP 1.0
          //header("Expires: 0"); // Proxies

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
          res.setHeader('Content-disposition', 'attachment; filename=CO_DemogExport.csv')
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
      var client = new pg.Client(conString);
      console.log("a:", conString);

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
      var client = new pg.Client(conString);
      console.log("b:", conString);

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

}
