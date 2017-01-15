//MICROSERVICE for getting data triggered by an advanced query

//returns from: http://nodejs-server-royhobbstn.c9users.io/geojson?db=acs1115&schema=data&sumlev=50&limit=1&table=b19013&bb=-144.5361328125,25.660940844615375,-60.1611328125,49.10662334783962&zoom=6&moe=yes
/*
{ "type": "FeatureCollection", "features": [ {"type": "Feature", "geometry": {"type":"MultiPolygon","coordinates":[[[[-105.3991,39.5821],[-105.3978,39.9129],[-105.1471,39.9139],[-105.1652,39.8915],[-105.1349,39.8893],[-105.1471,39.9139],[-105.0529,39.9142],[-105.0533,39.6678],[-105.0815,39.6677],[-105.0537,39.6512],[-105.1099,39.627],[-105.0534,39.6214],[-105.0487,39.5661],[-105.0868,39.4934],[-105.135,39.4708],[-105.1236,39.4341],[-105.171,39.4078],[-105.1666,39.3618],[-105.2178,39.2601],[-105.3292,39.1297],[-105.3979,39.1296],[-105.3991,39.5821]]]]}, "properties": {"geoname":"Jefferson","geonum":"108059","b19013001":"70164","b19013_moe001":"712"}} ]}
*/
'use strict';

const logger = require('../helpers/logger.js');

const setDefaultSchema = require('../helpers/helpers.js').setDefaultSchema;
const getMOE = require('../helpers/helpers.js').getMOE;
const getPretty = require('../helpers/helpers.js').getPretty;
const getMeta = require('../helpers/helpers.js').getMeta;
const getFields = require('../helpers/helpers.js').getFields;
const addMarginOfErrorFields = require('../helpers/helpers.js').addMarginOfErrorFields;
const getTableArray = require('../helpers/helpers.js').getTableArray;
const sendToDatabase = require('../helpers/helpers.js').sendToDatabase;
const sqlList = require('../helpers/helpers.js').sqlList;


var explode = require("locutus/php/strings/explode");
var substr = require("locutus/php/strings/substr");


module.exports = function (app) {


  app.get('/geojson', function (req, res) {

    // set defaults on parameters and assign them to an object called query_parameter
    const query_parameter = getGeoJsonParams(req, res);


    getFields(query_parameter)
      .then(mainGeoQuery)
      .then(assembleGeoJsonResponse);



    function mainGeoQuery(field_list) {
      var joinlist = ""; //working string of 'where' condition to be inserted into sql query

      logger.info('mainGeoQuery');
      logger.info('field list: ' + field_list);

      query_parameter.field = addMarginOfErrorFields(field_list, query_parameter.moe);
      logger.info(query_parameter.field);

      const table_array = getTableArray(query_parameter.field);
      logger.info(table_array);

      let join_table_list = ""; // working string of tables to be inserted into sql query

      // create a string to add to sql statement
      table_array.forEach(d => {
        join_table_list = `${join_table_list} natural join ${query_parameter.schema}.${d}`;
      });

      logger.info(join_table_list);

      //CASE 1:  you have a geonum
      //essentially you don't care about anything else.  just get the data for that/those geonum(s)
      if (query_parameter.geonum) {
        logger.info('CASE 1');

        //break the comma delimited records from geonum into an array  
        var geonumarray = explode(",", query_parameter.geonum);

        //quick sidestep, calculate number of digits in geonum to assign join table geography
        //because maybe sumlev wasn't given
        if (geonumarray[0].length === 3) {
          query_parameter.geodesc = 'state';
        }
        if (geonumarray[0].length === 6) {
          query_parameter.geodesc = 'county';
        }
        if (geonumarray[0].length === 8) {
          query_parameter.geodesc = 'place';
        }
        if (geonumarray[0].length === 12) {
          query_parameter.geodesc = 'tract';
        }
        if (geonumarray[0].length === 13) {
          query_parameter.geodesc = 'bg';
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
        logger.info('CASE 2');

        //CASE 2:  you have a geoid

        //break the comma delimited records from geonum into an array  
        var geoidarray = explode(",", query_parameter.geoid);

        //quick sidestep, calculate number of digits in geoid to assign join table geography
        if (geoidarray[0].length === 2) {
          query_parameter.geodesc = 'state';
        }
        if (geoidarray[0].length === 5) {
          query_parameter.geodesc = 'county';
        }
        if (geoidarray[0].length === 7) {
          query_parameter.geodesc = 'place';
        }
        if (geoidarray[0].length === 11) {
          query_parameter.geodesc = 'tract';
        }
        if (geoidarray[0].length === 12) {
          query_parameter.geodesc = 'bg';
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
        logger.info('CASE 3');

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
        logger.info('CASE 4');

        //CASE 4: Only Sumlev
        joinlist = " 5=5 "; //nonsense here because of preceding 'AND'
        //END CASE 4
      }
      else {
        logger.info('CASE 5');

        // CASE 5: No Geo
        logger.error('error: case 5');
        return;
        //END CASE 5
      }



      var bbstr = ""; //bounding box string

      //potential single select
      if (query_parameter.bb) {
        bbstr = query_parameter.geo + "." + query_parameter.geodesc + ".geom && ST_MakeEnvelope(" + query_parameter.bb + ", 4326) and ";
      } //bounding box example: "-105,40,-104,39" no spaces no quotes

      //CONSTRUCT MAIN SQL STATEMENT
      // execute query
      var sql = "SELECT geoname, geonum, " + query_parameter.field + ", st_asgeojson(st_transform(ST_Simplify((\"geom\")," + query_parameter.tolerance + "),4326),4) AS geojson from " + query_parameter.geo + "." + query_parameter.geodesc + " " + join_table_list + " where " + bbstr + " " + joinlist + " limit " + query_parameter.limit + ";";


      return sendToDatabase(sql, query_parameter.db);

    }



    function assembleGeoJsonResponse(resultdata) {


      let output = '';
      let rowOutput = '';

      for (let t = 0; t < resultdata.length; t++) {

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

    } // assembleGeoJsonResponse



  }); // end route


}; // end module



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

  obj.geo = setGeographyDataset(obj.db); // tiger, carto, or nhgis
  obj.geodesc = getGeoDesc(req.query.sumlev); // literal name of geography table being queried
  obj.tolerance = getTolerance(obj.zoom); // for simplifying geometry

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

function setGeographyDataset(db) {
  // carto,tiger or nhgis
  let geo;
  if (db === 'c1990' || db === 'c1980') {
    geo = 'nhgis';
  }
  else {
    geo = "carto";
  }
  return geo;
}

function getGeoDesc(sumlev) {
  let geodesc;

  if (sumlev == '160') {
    geodesc = 'place';
  }
  if (sumlev == '150') {
    geodesc = 'bg';
  }
  if (sumlev == '140') {
    geodesc = 'tract';
  }
  if (sumlev == '50') {
    geodesc = 'county';
  }
  if (sumlev == '40') {
    geodesc = 'state';
  }

  return geodesc;
}


function getTolerance(zoom) {

  let tolerance = 0;

  if (zoom === 2) {
    tolerance = 0.2;
  }
  if (zoom === 3) {
    tolerance = 0.1;
  }
  if (zoom === 4) {
    tolerance = 0.07;
  }
  if (zoom === 5) {
    tolerance = 0.04;
  }
  if (zoom === 6) {
    tolerance = 0.018;
  }
  if (zoom === 7) {
    tolerance = 0.01;
  }
  if (zoom === 8) {
    tolerance = 0.005;
  }
  if (zoom === 9) {
    tolerance = 0.003;
  }
  if (zoom === 10) {
    tolerance = 0.0015;
  }
  if (zoom === 11) {
    tolerance = 0.001;
  }
  if (zoom === 12) {
    tolerance = 0.0005;
  }
  if (zoom === 13) {
    tolerance = 0.00025;
  }
  if (zoom === 14) {
    tolerance = 0.0001;
  }
  if (zoom === 15) {
    tolerance = 0.0001;
  }
  if (zoom === 16) {
    tolerance = 0.0001;
  }

  return tolerance;
}
