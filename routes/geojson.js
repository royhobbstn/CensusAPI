// MICROSERVICE for retrieving geojson

// http://nodejs-server-royhobbstn.c9users.io/geojson?db=acs1115&schema=data&sumlev=50&limit=1&table=b19013&bb=-144.5361328125,25.660940844615375,-60.1611328125,49.10662334783962&zoom=6&moe=yes

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
const getWhereClause = require('../helpers/helpers.js').getWhereClause;
const createJoinTableList = require('../helpers/helpers.js').createJoinTableList;



// TODO logging && logging levels


module.exports = (app) => {


  app.get('/geojson', function (req, res) {

    // set defaults on parameters and assign them to an object called query_parameter
    const query_parameter = getGeoJsonParams(req, res);


    getFields(query_parameter)
      .then(mainGeoQuery)
      .then(assembleGeoJsonResponse);



    function mainGeoQuery(field_list) {

      logger.info('mainGeoQuery');
      logger.info('field list: ' + field_list);

      query_parameter.field = addMarginOfErrorFields(field_list, query_parameter.moe);
      logger.info(query_parameter.field);

      const table_array = getTableArray(query_parameter.field);

      const join_table_list = createJoinTableList(table_array, query_parameter.schema);

      let where_clause = getWhereClause(query_parameter); // working string of 'where' condition to be inserted into sql query

      if (where_clause === "") {
        // CASE 4: No Geo
        logger.warn('No Geography in query.  Please specify either a geoname, geonum, sumlev, county, or state.');
      }

      let bbstr = createBbString(query_parameter);

      //CONSTRUCT MAIN SQL STATEMENT
      // execute query
      var sql = "SELECT geoname, geonum, " + query_parameter.field + ", st_asgeojson(st_transform(ST_Simplify((\"geom\")," + query_parameter.tolerance + "),4326),4) AS geojson from " + query_parameter.geo + "." + query_parameter.geodesc + " " + join_table_list + " where " + bbstr + " " + where_clause + " limit " + query_parameter.limit + ";";

      return sendToDatabase(sql, query_parameter.db);
    }



    function assembleGeoJsonResponse(resultdata) {

      let output = '';
      let rowOutput = '';

      resultdata.forEach(function (d) {
        rowOutput = (rowOutput.length > 0 ? ',' : '') + '{"type": "Feature", "geometry": ' + d['geojson'] + ', "properties": {';
        let props = '';
        let id = '';

        const keys = Object.keys(d);

        keys.forEach(function (key) {
          if (key !== "geojson") {
            props = props + (props.length > 0 ? ',' : '') + '"' + key + '":"' + d[key] + '"';
          }
          if (key === "id") {
            id = id + ',"id":"' + d[key] + '"';
          }
        });

        rowOutput = rowOutput + props + '}';
        rowOutput = rowOutput + id;
        rowOutput = rowOutput + '}';
        output = output + rowOutput;
      });

      const send_geojson = '{ "type": "FeatureCollection", "features": [ ' + output + ' ]}';

      res.set({
        "Content-Type": "application/json"
      });
      res.send(send_geojson);

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
  obj.table = req.query.table;
  obj.zoom = req.query.zoom || 16;
  obj.bb = req.query.bb;

  // sumlev for geo is treated differently than in demog.js since there is no 'sumlev'
  // field to join to in the geography tables
  obj.sumlev = undefined;
  obj.geosumlev = req.query.sumlev;

  obj.db = req.query.db || 'acs1115';

  // set default for schema if it is missing
  obj.schema = req.query.schema || setDefaultSchema(obj.db);

  // by default limits to 1000 search results.  override by setting limit= in GET string
  obj.limit = parseInt(req.query.limit, 10) || 1000;

  obj.moe = getMOE(obj.db, req.query.moe);
  obj.pretty = getPretty(req.query.pretty);
  obj.meta = getMeta(req.query.meta);

  obj.geo = setGeographyDataset(obj.db); // tiger, carto, or nhgis
  obj.geodesc = getGeoDesc(obj.geosumlev, req.query.geoid, req.query.geonum); // literal name of geography table being queried
  obj.tolerance = getTolerance(obj.zoom); // for simplifying geometry

  return obj;

}

function checkValidGeoJsonParams(query_params) {
  // get list of parameters - check all are valid

  const keys = Object.keys(query_params);

  const valid_keys = ['pretty', 'field', 'state', 'county', 'sumlev', 'place', 'geonum', 'geoid', 'db', 'schema', 'type', 'limit', 'moe', 'table', 'meta', 'zoom', 'bb'];

  keys.forEach(key => {
    if (!valid_keys.includes(key)) {
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

function getGeoDesc(geosumlev, geoid, geonum) {
  let geodesc;

  if (geosumlev === '160') {
    geodesc = 'place';
  }
  if (geosumlev === '150') {
    geodesc = 'bg';
  }
  if (geosumlev === '140') {
    geodesc = 'tract';
  }
  if (geosumlev === '50') {
    geodesc = 'county';
  }
  if (geosumlev === '40') {
    geodesc = 'state';
  }

  if (!geosumlev && geonum) {
    geodesc = getGeodescFromGeonum(geonum);
  }
  else if (!geosumlev && geoid) {
    geodesc = getGeodescFromGeoid(geoid);
  }

  return geodesc;
}

function getGeodescFromGeoid(geoid) {
  let geodesc = "";

  // if geosumlev not specified, get geodesc from geoid  
  const geoid_item = geoid.split(',')[0];

  //quick sidestep, calculate number of digits in geoid to assign join table geography
  if (geoid_item.length === 2) {
    geodesc = 'state';
  }
  if (geoid_item.length === 5) {
    geodesc = 'county';
  }
  if (geoid_item.length === 7) {
    geodesc = 'place';
  }
  if (geoid_item.length === 11) {
    geodesc = 'tract';
  }
  if (geoid_item.length === 12) {
    geodesc = 'bg';
  }

  return geodesc;
}


function getGeodescFromGeonum(geonum) {
  let geodesc = "";

  // if geosumlev not specified, get geodesc from geoid  
  const geonum_item = geonum.split(',')[0];

  //quick sidestep, calculate number of digits in geonum to assign join table geography
  if (geonum_item.length === 3) {
    geodesc = 'state';
  }
  if (geonum_item.length === 6) {
    geodesc = 'county';
  }
  if (geonum_item.length === 8) {
    geodesc = 'place';
  }
  if (geonum_item.length === 12) {
    geodesc = 'tract';
  }
  if (geonum_item.length === 13) {
    geodesc = 'bg';
  }

  return geodesc;
}


function getTolerance(zoom) {
  // lookup for tolerance values (1-16 are valid.  everything else will return with 0);
  const lookup = [undefined, 0.2, 0.2, 0.1, 0.07, 0.04, 0.018, 0.01, 0.005, 0.003, 0.0015, 0.001, 0.0005, 0.00025, 0.0001, 0.0001, 0.0001];

  return lookup[parseInt(zoom, 10)] || 0;
}


function createBbString(query_parameter) {
  let bbstr = ""; //bounding box string

  //potential single select
  if (query_parameter.bb) {
    bbstr = `${query_parameter.geo}.${query_parameter.geodesc}.geom && ST_MakeEnvelope(${query_parameter.bb}, 4326) and `;
  } //bounding box example: "-105,40,-104,39" no spaces no quotes

  return bbstr;
}
