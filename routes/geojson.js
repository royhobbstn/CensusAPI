//MICROSERVICE for getting data triggered by an advanced query

//returns from: http://nodejs-server-royhobbstn.c9users.io/geojson?db=acs1115&schema=data&sumlev=50&limit=1&table=b19013&bb=-144.5361328125,25.660940844615375,-60.1611328125,49.10662334783962&zoom=6&moe=yes
/*
{ "type": "FeatureCollection", "features": [ {"type": "Feature", "geometry": {"type":"MultiPolygon","coordinates":[[[[-105.3991,39.5821],[-105.3978,39.9129],[-105.1471,39.9139],[-105.1652,39.8915],[-105.1349,39.8893],[-105.1471,39.9139],[-105.0529,39.9142],[-105.0533,39.6678],[-105.0815,39.6677],[-105.0537,39.6512],[-105.1099,39.627],[-105.0534,39.6214],[-105.0487,39.5661],[-105.0868,39.4934],[-105.135,39.4708],[-105.1236,39.4341],[-105.171,39.4078],[-105.1666,39.3618],[-105.2178,39.2601],[-105.3292,39.1297],[-105.3979,39.1296],[-105.3991,39.5821]]]]}, "properties": {"geoname":"Jefferson","geonum":"108059","b19013001":"70164","b19013_moe001":"712"}} ]}
*/

const logger = require('../helpers/logger');

const setDefaultSchema = require('../helpers/helpers.js').setDefaultSchema;
const getMOE = require('../helpers/helpers.js').getMOE;
const getPretty = require('../helpers/helpers.js').getPretty;
const onlyUnique = require('../helpers/helpers.js').onlyUnique;
const getMeta = require('../helpers/helpers.js').getMeta;

var strpos = require("locutus/php/strings/strpos");
var substr_replace = require("locutus/php/strings/substr_replace");
var explode = require("locutus/php/strings/explode");
var substr = require("locutus/php/strings/substr");
var implode = require("locutus/php/strings/implode");
var array_merge = require("locutus/php/array/array_merge");

module.exports = function (app, pg, conString) {


  app.get('/geojson', function (req, res) {

    // set defaults on parameters and assign them to an object called query_parameter
    const query_parameter = getGeoJsonParams(req, res);


    // carto,tiger or nhgis
    var geo;
    if (query_parameter.db === 'c1990' || query_parameter.db === 'c1980') {
      geo = 'nhgis';
    }
    else {
      geo = "carto";
    }


    //carto or tiger or nhgis
    var geodesc = ""; //for now, geodesc will be set based upon sumlev

    if (query_parameter.sumlev == '160') {
      geodesc = 'place';
    }
    if (query_parameter.sumlev == '150') {
      geodesc = 'bg';
    }
    if (query_parameter.sumlev == '140') {
      geodesc = 'tract';
    }
    if (query_parameter.sumlev == '50') {
      geodesc = 'county';
    }
    if (query_parameter.sumlev == '40') {
      geodesc = 'state';
    }

    //by default limits to 1000 search results.  override by setting limit= in GET string
    var limit = parseInt(req.query.limit, 10) || 1000;

    //if database is acs, check to see if moe option is flagged
    var moe = 'no';
    if (query_parameter.db == 'acs0812' || query_parameter.db == 'acs0913' || query_parameter.db == 'acs1014' || query_parameter.db == 'acs1115') {
      if (req.query.moe) {
        moe = req.query.moe;
      }
    }


    var tolerance = 0; //for simplifying geometry

    if (query_parameter.zoom == 2) {
      tolerance = 0.2;
    }
    if (query_parameter.zoom == 3) {
      tolerance = 0.1;
    }
    if (query_parameter.zoom == 4) {
      tolerance = 0.07;
    }
    if (query_parameter.zoom == 5) {
      tolerance = 0.04;
    }
    if (query_parameter.zoom == 6) {
      tolerance = 0.018;
    }
    if (query_parameter.zoom == 7) {
      tolerance = 0.01;
    }
    if (query_parameter.zoom == 8) {
      tolerance = 0.005;
    }
    if (query_parameter.zoom == 9) {
      tolerance = 0.003;
    }
    if (query_parameter.zoom == 10) {
      tolerance = 0.0015;
    }
    if (query_parameter.zoom == 11) {
      tolerance = 0.001;
    }
    if (query_parameter.zoom == 12) {
      tolerance = 0.0005;
    }
    if (query_parameter.zoom == 13) {
      tolerance = 0.00025;
    }
    if (query_parameter.zoom == 14) {
      tolerance = 0.0001;
    }
    if (query_parameter.zoom == 15) {
      tolerance = 0.0001;
    }
    if (query_parameter.zoom == 16) {
      tolerance = 0.0001;
    }


    //variables and arrays to use later
    var tablelist = []; //array of all tables used in query
    var jointablelist = ""; //working string of tables to be inserted into sql query
    var joinlist = ""; //working string of 'where' condition to be inserted into sql query

    var moefields = []; //moe field array
    var tcolumns = []; //columns gathered from table(s?)
    var ttlfields = [];


    //if no fields are selected (then a table must be).  Create fields list based on the given table.

    //CRITICAL - FOR FASTEST PERFORMANCE, explicitly name fields.  
    //Otherwise, you must go through this loop to look up fields by querying database.
    //OPTION: perhaps conditionally load table-field lookup server-side
    //table lookup adds around 250ms to 300ms extra
    if (!query_parameter.field) {

      if (query_parameter.table) {

        var atablearray = explode(",", query_parameter.table);
        var atablestr = "";

        for (var i = 0; i < atablearray.length; i++) {
          atablestr = atablestr + " table_name='" + atablearray[i] + "' or";
        }

        //trim last trailing 'or'
        atablestr = substr(atablestr, 0, -2);

        //STOP: Don't Query DB for this!!! Too Slow!!!
        var tablesql = "SELECT column_name from information_schema.columns where (" + atablestr + ") and table_schema='" + query_parameter.schema + "';";

        query_parameter.field = sendinternal(tablesql); //ASYNC



      } //end if table
    } //end if field defined



    function check() {
      if (!query_parameter.field) {
        setTimeout(check, 50);
        //console.log('waiting...');
      }
      else {
        //console.log('field: '+field);
        continue_program();
      }
    }

    check();


    function continue_program() {

      // we have fields: either hand entered or derived from tables


      //break the comma delimited records from field into an array  
      ttlfields = explode(",", query_parameter.field);


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

        ttlfields = ttlfields.filter(onlyUnique);

        //send moe modified field list back to main field list
        query_parameter.field = implode(',', ttlfields);
      }


      //get a list of tables based upon characters in each field name  (convention: last 3 characters identify field number, previous characters are table name) 
      for (var m = 0; m < ttlfields.length; m++) {
        tablelist.push(substr(ttlfields[m], 0, -3));
      }

      //remove duplicate tables in array
      tablelist = tablelist.filter(onlyUnique);

      //create a string to add to sql statement
      for (var n = 0; n < tablelist.length; n++) {
        jointablelist = jointablelist + " natural join " + query_parameter.schema + "." + tablelist[n];
      }


      //CASE 1:  you have a geonum
      //essentially you don't care about anything else.  just get the data for that/those geonum(s)
      if (query_parameter.geonum) {

        //break the comma delimited records from geonum into an array  
        var geonumarray = explode(",", query_parameter.geonum);

        //quick sidestep, calculate number of digits in geonum to assign join table geography
        //because maybe sumlev wasn't given
        if (geonumarray[0].length === 3) {
          geodesc = 'state';
        }
        if (geonumarray[0].length === 6) {
          geodesc = 'county';
        }
        if (geonumarray[0].length === 8) {
          geodesc = 'place';
        }
        if (geonumarray[0].length === 12) {
          geodesc = 'tract';
        }
        if (geonumarray[0].length === 13) {
          geodesc = 'bg';
        }

        //iterate through all geonum's
        for (var z = 0; z < geonumarray.length; z++) {
          joinlist = joinlist + " geonum=" + geonumarray[z] + " or";
        }

        //trim last trailing 'or'
        joinlist = substr(joinlist, 0, -2);

        //END CASE 1
      }
      else if (query_parameter.geoid) {
        //CASE 2:  you have a geoid

        //break the comma delimited records from geonum into an array  
        var geoidarray = explode(",", query_parameter.geoid);

        //quick sidestep, calculate number of digits in geoid to assign join table geography
        if (geoidarray[0].length === 2) {
          geodesc = 'state';
        }
        if (geoidarray[0].length === 5) {
          geodesc = 'county';
        }
        if (geoidarray[0].length === 7) {
          geodesc = 'place';
        }
        if (geoidarray[0].length === 11) {
          geodesc = 'tract';
        }
        if (geoidarray[0].length === 12) {
          geodesc = 'bg';
        }

        //iterate through all geoids, simply put a '1' in front and treat them like geonums
        for (var y = 0; y < geoidarray.length; y++) {
          joinlist = joinlist + " geonum=1" + geoidarray[y] + " or";
        }

        //trim last trailing 'or'
        joinlist = substr(joinlist, 0, -2);

        //END CASE 2  
      }
      else if (query_parameter.county || query_parameter.state) {
        //CASE 3 - query

        var condition = ""; //condition is going to be a 3 character string which identifies sumlev, county, state (yes/no) (1,0)
        if (query_parameter.county) {
          condition = "1";
        }
        else {
          condition = "0";
        }
        if (query_parameter.state) {
          condition = condition + "1";
        }
        else {
          condition = condition + "0";
        }


        if (query_parameter.county) {
          //create county array out of delimited list
          var countylist = "";

          //break the comma delimited records from county into an array  
          var countyarray = explode(",", query_parameter.county);

          //iterate through all counties
          for (var x = 0; x < countyarray.length; x++) {
            countylist = countylist + " county=" + countyarray[x] + " or";
          }

          //trim last trailing 'or'
          countylist = substr(countylist, 0, -2);
        }


        if (query_parameter.state) {
          //create state array out of delimited list
          var statelist = "";

          //break the comma delimited records from county into an array  
          var statearray = explode(",", query_parameter.state);

          //iterate through all states
          for (var u = 0; u < statearray.length; u++) {
            statelist = statelist + " state=" + statearray[u] + " or";
          }

          //trim last trailing 'or'
          statelist = substr(statelist, 0, -2);
        }


        //every possible combination of county, state
        if (condition === '01') {
          joinlist = " (" + statelist + ") ";
        }
        if (condition === '11') {
          joinlist = " (" + countylist + ") and (" + statelist + ") ";
        }
        if (condition === '10') {
          joinlist = " (" + countylist + ") ";
        }
        //END CASE 3
      }
      else if (query_parameter.sumlev) {
        //CASE 4: Only Sumlev
        joinlist = " 5=5 "; //nonsense here because of preceding 'AND'
        //END CASE 4
      }
      else {
        // CASE 5: No Geo
        console.log('error: case 5');
        return;
        //END CASE 5
      }



      var bbstr = ""; //bounding box string

      //potential single select
      if (query_parameter.bb) {
        bbstr = geo + "." + geodesc + ".geom && ST_MakeEnvelope(" + query_parameter.bb + ", 4326) and ";
      } //bounding box example: "-105,40,-104,39" no spaces no quotes

      //CONSTRUCT MAIN SQL STATEMENT
      // execute query
      var sql = "SELECT geoname, geonum, " + query_parameter.field + ", st_asgeojson(st_transform(ST_Simplify((\"geom\")," + tolerance + "),4326),4) AS geojson from " + geo + "." + geodesc + " " + jointablelist + " where " + bbstr + " " + joinlist + " limit " + limit + ";";

      console.log(sql);

      sendtodatabase(sql);


    }


    function sendtodatabase(sqlstring) {

      var client = new pg.Client(conString + query_parameter.db);

      client.connect(function (err) {

        if (err) {
          return console.error('could not connect to postgres', err);
        }

        client.query(sqlstring, function (err, result) {

          if (err) {
            return console.error('error running query', err);
          }

          var resultdata = result.rows;


          // Build GeoJSON
          var output = '';
          var rowOutput = '';

          for (var t = 0; t < resultdata.length; t++) {

            rowOutput = (rowOutput.length > 0 ? ',' : '') + '{"type": "Feature", "geometry": ' + resultdata[t]['geojson'] + ', "properties": {';
            var props = '';
            var id = '';


            for (var key in resultdata[t]) {
              if (resultdata[t].hasOwnProperty(key)) {

                if (key !== "geojson") {
                  props = props + (props.length > 0 ? ',' : '') + '"' + key + '":"' + resultdata[t][key] + '"';
                }
                if (key === "id") {
                  id = id + ',"id":"' + resultdata[t][key] + '"';
                }

              }
            }

            rowOutput = rowOutput + props + '}';
            rowOutput = rowOutput + id;
            rowOutput = rowOutput + '}';
            output = output + rowOutput;



          }

          var arroutput = '{ "type": "FeatureCollection", "features": [ ' + output + ' ]}';

          res.set({
            "Content-Type": "application/json"
          });
          res.send(arroutput);


          client.end();

        });
      });
    } // send to database


    function sendinternal(sqlstring) {

      var client = new pg.Client(conString + query_parameter.db);

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



          for (var i = 0; i < tableresult.length; i++) {
            if (tableresult[i].column_name !== 'geonum') {
              tcolumns.push(tableresult[i].column_name);
            }
          }

          query_parameter.field = implode(',', tcolumns); //$field becomes fields queried from info schema based upon table

          return query_parameter.field;



        });
      });
    } // send internal





  });


};



function getGeoJsonParams(req) {

  checkValidGeoJsonParams(req.query);

  const obj = {};

  // potential multi select (comma delimited list)
  obj.field = req.query.field;
  obj.state = req.query.state;
  obj.county = req.query.county;
  obj.geonum = req.query.geonum;
  obj.geoid = req.query.geoid;
  obj.sumlev = req.query.sumlev;
  obj.table = req.query.table;
  obj.zoom = req.query.zoom || 16;
  obj.bb = req.query.bb;

  obj.db = req.query.db || 'acs1115';

  // set default for schema if it is missing
  obj.schema = req.query.schema || setDefaultSchema(obj.db);

  // by default limits to 1000 search results.  override by setting limit= in GET string
  obj.limit = parseInt(req.query.limit, 10) || 1000;

  obj.moe = getMOE(obj.db, req.query.moe);
  obj.pretty = getPretty(req.query.pretty);
  obj.meta = getMeta(req.query.meta);

  return obj;

}

function checkValidGeoJsonParams(query_params) {
  // get list of parameters - check all are valid

  const keys = Object.keys(query_params);

  const valid_keys = ['pretty', 'field', 'state', 'county', 'sumlev', 'place', 'geonum', 'geoid', 'db', 'schema', 'type', 'limit', 'moe', 'table', 'meta', 'zoom', 'bb'];

  keys.forEach(key => {
    if (valid_keys.indexOf(key) === -1) {
      logger.warn(`Your parameter '${key}' is not valid.`);
    }
  });

}
