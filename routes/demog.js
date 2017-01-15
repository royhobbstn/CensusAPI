// returns from: http://nodejs-server-royhobbstn.c9users.io/demog?db=acs1115&schema=data&table=b19013&moe=yes&geonum=108037,108039&type=json

/*
{"source":"acs1115","schema":"data","tablemeta":[{"table_id":"b19013","table_title":"MEDIAN HOUSEHOLD INCOME IN THE PAST 12 MONTHS (IN 2015 INFLATION-ADJUSTED DOLLARS)","universe":"Universe:  Households"}],"fieldmeta":[{"column_id":"b19013001","column_title":"Median household income in the past 12 months (in 2015 Inflation-adjusted dollars)"}],"data":[{"geoname":"Eagle County, Colorado","state":"8","county":"37","place":null,"tract":null,"bg":null,"geonum":108037,"geoid":"08037","b19013001":72214,"b19013_moe001":3789},{"geoname":"Elbert County, Colorado","state":"8","county":"39","place":null,"tract":null,"bg":null,"geonum":108039,"geoid":"08039","b19013001":84963,"b19013_moe001":4050}]}
*/

'use strict';

// V2 Breaking Changes
// --------------------
// GEOID added (string)
// GEONUM is now an integer
// STATE, COUNTY, PLACE now return full string equivalent (you can enter as integers, OR strings in query)
// all returned data is now returned as Numbers rather than Strings
// 'pretty' parameter added to optionally format json output
// 'meta' added to optionally exclude metadata output



// TODO test coverage (think of everywhere where you could cause a problem with the params)
// TODO error handling everywhere
// TODO interesting error case, &table=xkjd&table=sdfk creates an array of [xkjd,sdfk];

// TODO imagine extended geo situations
// TODO yank the database and see what happens

// TODO killer README with extended geo




const csv = require("express-csv"); // intellisense may flag this, but it is called

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
const pad = require('../helpers/helpers.js').pad;


module.exports = (app) => {

  app.get('/demog', (req, res) => {

    const start = process.hrtime();

    logger.info('**START**');

    // set defaults on parameters and assign them to an object called query_parameter
    const query_parameter = getParams(req);
    logger.info('Parameters acquired.');


    // program outline
    getFields(query_parameter) // get a full list of fields requested
      .then(parallelLoad) // run all queries
      .then(writeResponse) // return response back to user
      .catch(catchError); // or report error


    //
    function catchError(reason) {
      logger.error('There has been an error: ' + reason);
      logger.info('**FAILED**');
      res.status(500).send(reason);
    }


    //
    function writeResponse(success) {
      logger.info('Ready to assemble output.');
      // this is where you combine all the info together (meta from fields 
      // and tables, plus main query) into a final JS object to be returned
      let response = assembleOutput(success, query_parameter.type, query_parameter.meta, query_parameter.field, query_parameter.db, query_parameter.schema);
      logger.info(`--Time Elapsed: ${process.hrtime(start)[1] / 1000000}ms`);
      logger.info('**COMPLETED**');

      if (query_parameter.type === 'csv') {
        // send CSV
        res.setHeader('Content-disposition', `attachment; filename=${query_parameter.db}_export.csv`);
        res.csv(response);
      }
      else {
        // send JSON
        const pretty_print_json = query_parameter.pretty ? '  ' : '';
        response = (JSON.stringify(response, null, pretty_print_json));
        res.setHeader("Content-Type", "application/json");
        res.status(200).send(response);
      }
    }

    //
    function parallelLoad(field_list) {
      // calls main query, retrieves field and table metadata
      // all async.  each of these calls is indpendent of each other
      // promise.all returns when all three queries (or one, if metadata is turned off) return
      query_parameter.field = addMarginOfErrorFields(field_list, query_parameter.moe);
      logger.info('Field Parameter Set');

      const promise_array = [];

      promise_array[0] = mainQuery(); // resolves to main query info

      // do not make extra calls if metadata is not wanted
      if (query_parameter.meta) {
        promise_array[1] = getFieldMeta(); // resolves to metadata info
        promise_array[2] = getTableMeta(); // resolves to table metadata info
      }
      else {
        logger.info('Skipping metadata requests.');
      }

      return Promise.all(promise_array);
    }



    //
    function mainQuery() {

      return new Promise((resolve, reject) => {

        let where_clause = ""; // working string of 'where' condition to be inserted into sql query

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

          const geonum_list = `1${query_parameter.geoid.replace(/,/g, ',1')}`; // convert geoids to geonums
          where_clause = sqlList(geonum_list, "geonum");
        }
        else if (query_parameter.sumlev || query_parameter.county || query_parameter.state) {
          // CASE 3 - query
          logger.info('CASE 3: Query using one or all of: sumlev, county, or state.');
          const condition = getConditionString(query_parameter.sumlev, query_parameter.county, query_parameter.state);

          const countylist = query_parameter.county ? sqlList(query_parameter.county, 'county') : '';
          const statelist = query_parameter.state ? sqlList(query_parameter.state, 'state') : '';
          const sumlevlist = query_parameter.sumlev ? sqlList(query_parameter.sumlev, 'sumlev') : '';

          where_clause = constructWhereClause(condition, statelist, countylist, sumlevlist);

        }
        else {
          // CASE 4: No Geo
          logger.warn('No Geography in query.  Please specify either a geoname, geonum, sumlev, county, or state.');
          reject('No Geography in query.  Please specify either a geoname, geonum, sumlev, county, or state.');
        }

        const table_array = getTableArray(query_parameter.field);

        let join_table_list = ""; // working string of tables to be inserted into sql query

        // create a string to add to sql statement
        table_array.forEach(d => {
          join_table_list = `${join_table_list} natural join ${query_parameter.schema}.${d}`;
        });

        // CONSTRUCT MAIN SQL STATEMENT
        const sql = `SELECT geoname, state, county, place, tract, bg, geonum, geoid, ${query_parameter.field} from search.${query_parameter.schema}${join_table_list} where${where_clause} limit ${query_parameter.limit};`;

        logger.info('Sending Main Query to the Database.');
        sendToDatabase(sql, query_parameter.db).then(success => {
          logger.info('Main Query result successfully returned.');
          resolve(success);
        }, failure => {
          logger.error('Main Query failed.');
          reject(failure);
        });

      });
    }





    //
    function getFieldMeta() {

      return new Promise((resolve, reject) => {

        const field_array = query_parameter.field.split(",");
        const field_clause = sqlList(field_array, 'column_id');

        const field_metadata_sql = `SELECT column_id, column_verbose from ${query_parameter.schema}.census_column_metadata where ${field_clause};`;

        logger.info('Sending request for field metadata to the database.');

        sendToDatabase(field_metadata_sql, query_parameter.db)
          .then(field_metadata_query_result => {

            const field_metadata_array = mapFieldMetadataQueryResult(field_metadata_query_result);

            logger.info('Your database request for field metadata has completed successfully.');
            resolve(field_metadata_array);

          }, failure => {
            logger.error('Your database request for field metadata has failed.');
            reject(failure);
          });

      }); // end new promise

    } // end getFieldMeta


    //
    function getTableMeta() {

      return new Promise((resolve, reject) => {

        const table_array = getTableArray(query_parameter.field);
        const table_clause = sqlList(table_array, 'table_id');

        const table_metadata_sql = `SELECT table_id, table_title, universe from ${query_parameter.schema}.census_table_metadata where${table_clause};`;

        logger.info('Sending request for table metadata to the database.');

        sendToDatabase(table_metadata_sql, query_parameter.db)
          .then(table_metadata_query_result => {

            const table_metadata_array = mapTableMetadataQueryResult(table_metadata_query_result);

            logger.info('Your database request for table metadata has completed successfully.');
            resolve(table_metadata_array);

          }, failure => {
            logger.error('Your database request for table metadata has failed.');
            reject(failure);
          });

      }); // end new promise

    } // end getTableMeta



  }); // end route


}; // end module exports













function getConditionString(sumlev, county, state) {
  // condition is going to be a 3 character string which identifies sumlev, county, state (yes/no) (1,0)

  let condition = "";
  sumlev ? condition = "1" : "0";
  county ? condition += "1" : condition += "0";
  state ? condition += "1" : condition += "0";

  return condition;
}


function getParams(req) {

  checkValidParams(req.query);

  const obj = {};

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
  obj.schema = req.query.schema || setDefaultSchema(obj.db);

  // by default limits to 1000 search results.  override by setting limit= in GET string
  obj.limit = parseInt(req.query.limit, 10) || 1000;

  obj.moe = getMOE(obj.db, req.query.moe);
  obj.pretty = getPretty(req.query.pretty);
  obj.meta = getMeta(req.query.meta);

  return obj;

}


function checkValidParams(query_params) {
  // get list of parameters - check all are valid

  const keys = Object.keys(query_params);

  const valid_keys = ['pretty', 'field', 'state', 'county', 'sumlev', 'place', 'geonum', 'geoid', 'db', 'schema', 'type', 'limit', 'moe', 'table', 'meta'];

  keys.forEach(key => {
    if (valid_keys.indexOf(key) === -1) {
      logger.warn(`Your parameter '${key}' is not valid.`);
    }
  });

}


function constructWhereClause(condition, statelist, countylist, sumlevlist) {

  let joinlist = "";
  logger.info('Constructing WHERE clause using a combination of state, county and sumlev.');

  switch (condition) {
  case '001':
    joinlist = ` (${statelist}) `;
    break;
  case '011':
    joinlist = ` (${countylist}) and (${statelist}) `;
    break;
  case '111':
    joinlist = ` (${sumlevlist}) and (${countylist}) and (${statelist}) `;
    break;
  case '010':
    joinlist = ` (${countylist}) `;
    break;
  case '110':
    joinlist = ` (${sumlevlist}) and (${countylist})`;
    break;
  case '100':
    joinlist = ` (${sumlevlist}) `;
    break;
  case '101':
    joinlist = ` (${sumlevlist}) and (${statelist}) `;
    break;
  }

  return joinlist;
}



function mapFieldMetadataQueryResult(field_metadata_query_result) {
  return field_metadata_query_result.map(d => {
    const temp_obj = {};
    temp_obj.column_id = d.column_id;
    temp_obj.column_title = d.column_verbose;
    return temp_obj;
  });
}


function stringifyStatePlaceCounty(query_data) {
  // convert non-null values of state, place, and county to their string equivalent
  return query_data.map(d => {
    d.state = (d.state !== null) ? pad(d.state, 2, '0') : null;
    d.place = (d.place !== null) ? pad(d.place, 5, '0') : null;
    d.county = (d.county !== null) ? pad(d.county, 3, '0') : null;
    return d;
  });
}


function mapTableMetadataQueryResult(table_metadata_query_result) {
  return table_metadata_query_result.map(d => {
    const temp_obj = {};
    temp_obj.table_id = d.table_id;
    temp_obj.table_title = d.table_title;
    temp_obj.universe = d.universe;
    return temp_obj;
  });
}





function assembleOutput(promise_output, type, meta, field, db, schema) {

  const query_data = promise_output[0]; // main data object

  if (meta) {
    var column_metadata = promise_output[1];
    var table_metadata = promise_output[2];
  }

  const data_array = stringifyStatePlaceCounty(query_data);

  if (type === 'csv') {
    return assembleCsvOutput(data_array, column_metadata, meta, field);
  }
  else {
    return assembleJsonOutput(table_metadata, column_metadata, data_array, meta, db, schema);
  }

}



function assembleCsvOutput(data_array, column_metadata, meta, field) {
  logger.info('Customizing for type CSV.');

  // data_array changes from array of objects to array of arrays
  const data_array_as_csv_array = data_array.map(d => {
    const keys = Object.keys(d);
    const temp_array = keys.map(e => {
      return d[e];
    });
    return temp_array;
  });

  if (meta) {
    logger.info('Writing CSV metadata.');

    // array for csv field descriptions only
    const csv_metadata_row = column_metadata.map(d => {
      return d.column_title;
    });

    // add metadata to front (on second row)
    csv_metadata_row.unshift("Geographic Area Name", "State FIPS", "County FIPS", "Place FIPS", "Tract FIPS", "BG FIPS", "Unique ID", "GEOID");

    data_array_as_csv_array.unshift(csv_metadata_row);
  }
  else {
    logger.info('Excluding metadata row from CSV.');
  }


  const field_name_row = field.split(",");

  // add column names to front
  field_name_row.unshift("geoname", "state", "county", "place", "tract", "bg", "geonum", "geoid");
  data_array_as_csv_array.unshift(field_name_row);

  return data_array_as_csv_array;
}




function assembleJsonOutput(table_metadata, column_metadata, data_array, meta, db, schema) {
  // JSON Output (DEFAULT)
  logger.info('Customizing for type JSON');

  const json_result = {};
  json_result.source = db;
  json_result.schema = schema;

  if (meta) {
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
