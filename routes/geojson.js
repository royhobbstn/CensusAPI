//MICROSERVICE for getting data triggered by an advanced query

//returns from: /demogpost?db=acs1014&schema=data&table=b19013&moe=yes&geonum=108037,108039&type=csv
/*
{ source: 'acs1014', schema: 'data', tablemeta:  [ { table_id: 'b19013', table_title: 'MEDIAN HOUSEHOLD INCOME IN THE PAST 12 MONTHS (IN 2014 INFLATION-ADJUSTED DOLLARS)', universe: 'Universe:  Households' } ],  fieldmeta: [ { column_id: 'b19013001', column_title: 'Median household income in the past 12 months (in 2014 Inflation-adjusted dollars)' } ], data: [ { geoname: 'Eagle County, Colorado', state: '8', county: '37',  place: null, tract: null, bg: null, geonum: '108037', b19013001: '73774', b19013_moe001: '5282' }, { geoname: 'Elbert County, Colorado', state: '8', county: '39', place: null, tract: null, bg: null, geonum: '108039', b19013001: '82154', b19013_moe001: '4193' } ], error: [] }
*/

module.exports = function(app, pg, csv, conString){


app.get('/geojson', function(req, res) {

  var lastbranchdone=0;

//PHP.js
function strpos(haystack, needle, offset) {
  //  discuss at: http://phpjs.org/functions/strpos/
  // original by: Kevin van Zonneveld (http://kevin.vanzonneveld.net)
  // improved by: Onno Marsman
  // improved by: Brett Zamir (http://brett-zamir.me)
  // bugfixed by: Daniel Esteban
  //   example 1: strpos('Kevin van Zonneveld', 'e', 5);
  //   returns 1: 14

  var i = (haystack + '')
    .indexOf(needle, (offset || 0));
  return i === -1 ? false : i;
}

function str_replace(search, replace, subject, count) {
  //  discuss at: http://phpjs.org/functions/str_replace/
  // original by: Kevin van Zonneveld (http://kevin.vanzonneveld.net)
  // improved by: Gabriel Paderni
  // improved by: Philip Peterson
  // improved by: Simon Willison (http://simonwillison.net)
  // improved by: Kevin van Zonneveld (http://kevin.vanzonneveld.net)
  // improved by: Onno Marsman
  // improved by: Brett Zamir (http://brett-zamir.me)
  //  revised by: Jonas Raoni Soares Silva (http://www.jsfromhell.com)
  // bugfixed by: Anton Ongson
  // bugfixed by: Kevin van Zonneveld (http://kevin.vanzonneveld.net)
  // bugfixed by: Oleg Eremeev
  //    input by: Onno Marsman
  //    input by: Brett Zamir (http://brett-zamir.me)
  //    input by: Oleg Eremeev
  //        note: The count parameter must be passed as a string in order
  //        note: to find a global variable in which the result will be given
  //   example 1: str_replace(' ', '.', 'Kevin van Zonneveld');
  //   returns 1: 'Kevin.van.Zonneveld'
  //   example 2: str_replace(['{name}', 'l'], ['hello', 'm'], '{name}, lars');
  //   returns 2: 'hemmo, mars'

  var i = 0,
    j = 0,
    temp = '',
    repl = '',
    sl = 0,
    fl = 0,
    f = [].concat(search),
    r = [].concat(replace),
    s = subject,
    ra = Object.prototype.toString.call(r) === '[object Array]',
    sa = Object.prototype.toString.call(s) === '[object Array]';
  s = [].concat(s);
  if (count) {
    this.window[count] = 0;
  }

  for (i = 0, sl = s.length; i < sl; i++) {
    if (s[i] === '') {
      continue;
    }
    for (j = 0, fl = f.length; j < fl; j++) {
      temp = s[i] + '';
      repl = ra ? (r[j] !== undefined ? r[j] : '') : r[0];
      s[i] = (temp)
        .split(f[j])
        .join(repl);
      if (count && s[i] !== temp) {
        this.window[count] += (temp.length - s[i].length) / f[j].length;
      }
    }
  }
  return sa ? s : s[0];
}  
  
function substr_replace(str, replace, start, length) {
  //  discuss at: http://phpjs.org/functions/substr_replace/
  // original by: Brett Zamir (http://brett-zamir.me)

  if (start < 0) { // start position in str
    start = start + str.length;
  }
  length = length !== undefined ? length : str.length;
  if (length < 0) {
    length = length + str.length - start;
  }

  return str.slice(0, start) + replace.substr(0, length) + replace.slice(length) + str.slice(start + length);
}  

function explode(delimiter, string, limit) {
  //  discuss at: http://phpjs.org/functions/explode/
  // original by: Kevin van Zonneveld (http://kevin.vanzonneveld.net)
  //   example 1: explode(' ', 'Kevin van Zonneveld');
  //   returns 1: {0: 'Kevin', 1: 'van', 2: 'Zonneveld'}

  if (arguments.length < 2 || typeof delimiter === 'undefined' || typeof string === 'undefined') return null;
  if (delimiter === '' || delimiter === false || delimiter === null) return false;
  if (typeof delimiter === 'function' || typeof delimiter === 'object' || typeof string === 'function' || typeof string ===
    'object') {
    return {
      0: ''
    };
  }
  if (delimiter === true) delimiter = '1';

  // Here we go...
  delimiter += '';
  string += '';

  var s = string.split(delimiter);

  if (typeof limit === 'undefined') return s;

  // Support for limit
  if (limit === 0) limit = 1;

  // Positive limit
  if (limit > 0) {
    if (limit >= s.length) return s;
    return s.slice(0, limit - 1)
      .concat([s.slice(limit - 1)
        .join(delimiter)
      ]);
  }

  // Negative limit
  if (-limit >= s.length) return [];

  s.splice(s.length + limit);
  return s;
}
  
function substr(str, start, len) {
  //  discuss at: http://phpjs.org/functions/substr/
  //     version: 909.322
  // original by: Martijn Wieringa
  // bugfixed by: T.Wild
  // improved by: Onno Marsman
  // improved by: Brett Zamir (http://brett-zamir.me)
  //  revised by: Theriault
  //        note: Handles rare Unicode characters if 'unicode.semantics' ini (PHP6) is set to 'on'
  //   example 1: substr('abcdef', 0, -1);
  //   returns 1: 'abcde'
  //   example 2: substr(2, 0, -6);
  //   returns 2: false
  //   example 3: ini_set('unicode.semantics',  'on');
  //   example 3: substr('a\uD801\uDC00', 0, -1);
  //   returns 3: 'a'
  //   example 4: ini_set('unicode.semantics',  'on');
  //   example 4: substr('a\uD801\uDC00', 0, 2);
  //   returns 4: 'a\uD801\uDC00'
  //   example 5: ini_set('unicode.semantics',  'on');
  //   example 5: substr('a\uD801\uDC00', -1, 1);
  //   returns 5: '\uD801\uDC00'
  //   example 6: ini_set('unicode.semantics',  'on');
  //   example 6: substr('a\uD801\uDC00z\uD801\uDC00', -3, 2);
  //   returns 6: '\uD801\uDC00z'
  //   example 7: ini_set('unicode.semantics',  'on');
  //   example 7: substr('a\uD801\uDC00z\uD801\uDC00', -3, -1)
  //   returns 7: '\uD801\uDC00z'

  var i = 0,
    allBMP = true,
    es = 0,
    el = 0,
    se = 0,
    ret = '';
  str += '';
  var end = str.length;

  // BEGIN REDUNDANT
  this.php_js = this.php_js || {};
  this.php_js.ini = this.php_js.ini || {};
  // END REDUNDANT
  switch ((this.php_js.ini['unicode.semantics'] && this.php_js.ini['unicode.semantics'].local_value.toLowerCase())) {
    case 'on':
      // Full-blown Unicode including non-Basic-Multilingual-Plane characters
      // strlen()
      for (i = 0; i < str.length; i++) {
        if (/[\uD800-\uDBFF]/.test(str.charAt(i)) && /[\uDC00-\uDFFF]/.test(str.charAt(i + 1))) {
          allBMP = false;
          break;
        }
      }

      if (!allBMP) {
        if (start < 0) {
          for (i = end - 1, es = (start += end); i >= es; i--) {
            if (/[\uDC00-\uDFFF]/.test(str.charAt(i)) && /[\uD800-\uDBFF]/.test(str.charAt(i - 1))) {
              start--;
              es--;
            }
          }
        } else {
          var surrogatePairs = /[\uD800-\uDBFF][\uDC00-\uDFFF]/g;
          while ((surrogatePairs.exec(str)) != null) {
            var li = surrogatePairs.lastIndex;
            if (li - 2 < start) {
              start++;
            } else {
              break;
            }
          }
        }

        if (start >= end || start < 0) {
          return false;
        }
        if (len < 0) {
          for (i = end - 1, el = (end += len); i >= el; i--) {
            if (/[\uDC00-\uDFFF]/.test(str.charAt(i)) && /[\uD800-\uDBFF]/.test(str.charAt(i - 1))) {
              end--;
              el--;
            }
          }
          if (start > end) {
            return false;
          }
          return str.slice(start, end);
        } else {
          se = start + len;
          for (i = start; i < se; i++) {
            ret += str.charAt(i);
            if (/[\uD800-\uDBFF]/.test(str.charAt(i)) && /[\uDC00-\uDFFF]/.test(str.charAt(i + 1))) {
              se++; // Go one further, since one of the "characters" is part of a surrogate pair
            }
          }
          return ret;
        }
        break;
      }
      // Fall-through
    case 'off':
      // assumes there are no non-BMP characters;
      //    if there may be such characters, then it is best to turn it on (critical in true XHTML/XML)
    default:
      if (start < 0) {
        start += end;
      }
      end = typeof len === 'undefined' ? end : (len < 0 ? len + end : len + start);
      // PHP returns false if start does not fall within the string.
      // PHP returns false if the calculated end comes before the calculated start.
      // PHP returns an empty string if start and end are the same.
      // Otherwise, PHP returns the portion of the string from start to end.
      return start >= str.length || start < 0 || start > end ? !1 : str.slice(start, end);
  }
  return undefined; // Please Netbeans
}
  
function implode(glue, pieces) {
  //  discuss at: http://phpjs.org/functions/implode/
  // original by: Kevin van Zonneveld (http://kevin.vanzonneveld.net)
  // improved by: Waldo Malqui Silva
  // improved by: Itsacon (http://www.itsacon.net/)
  // bugfixed by: Brett Zamir (http://brett-zamir.me)
  //   example 1: implode(' ', ['Kevin', 'van', 'Zonneveld']);
  //   returns 1: 'Kevin van Zonneveld'
  //   example 2: implode(' ', {first:'Kevin', last: 'van Zonneveld'});
  //   returns 2: 'Kevin van Zonneveld'

  var i = '',
    retVal = '',
    tGlue = '';
  if (arguments.length === 1) {
    pieces = glue;
    glue = '';
  }
  if (typeof pieces === 'object') {
    if (Object.prototype.toString.call(pieces) === '[object Array]') {
      return pieces.join(glue);
    }
    for (i in pieces) {
      retVal += tGlue + pieces[i];
      tGlue = glue;
    }
    return retVal;
  }
  return pieces;
}  
  
function array_merge() {
  //  discuss at: http://phpjs.org/functions/array_merge/
  // original by: Brett Zamir (http://brett-zamir.me)
  // bugfixed by: Nate
  // bugfixed by: Brett Zamir (http://brett-zamir.me)
  //    input by: josh
  //   example 1: arr1 = {"color": "red", 0: 2, 1: 4}
  //   example 1: arr2 = {0: "a", 1: "b", "color": "green", "shape": "trapezoid", 2: 4}
  //   example 1: array_merge(arr1, arr2)
  //   returns 1: {"color": "green", 0: 2, 1: 4, 2: "a", 3: "b", "shape": "trapezoid", 4: 4}
  //   example 2: arr1 = []
  //   example 2: arr2 = {1: "data"}
  //   example 2: array_merge(arr1, arr2)
  //   returns 2: {0: "data"}

  var args = Array.prototype.slice.call(arguments),
    argl = args.length,
    arg,
    retObj = {},
    k = '',
    argil = 0,
    j = 0,
    i = 0,
    ct = 0,
    toStr = Object.prototype.toString,
    retArr = true;

  for (i = 0; i < argl; i++) {
    if (toStr.call(args[i]) !== '[object Array]') {
      retArr = false;
      break;
    }
  }

  if (retArr) {
    retArr = [];
    for (i = 0; i < argl; i++) {
      retArr = retArr.concat(args[i]);
    }
    return retArr;
  }

  for (i = 0, ct = 0; i < argl; i++) {
    arg = args[i];
    if (toStr.call(arg) === '[object Array]') {
      for (j = 0, argil = arg.length; j < argil; j++) {
        retObj[ct++] = arg[j];
      }
    } else {
      for (k in arg) {
        if (arg.hasOwnProperty(k)) {
          if (parseInt(k, 10) + '' === k) {
            retObj[ct++] = arg[k];
          } else {
            retObj[k] = arg[k];
          }
        }
      }
    }
  }
  return retObj;
}  
  
function array_unshift(array) {
  //  discuss at: http://phpjs.org/functions/array_unshift/
  // original by: Kevin van Zonneveld (http://kevin.vanzonneveld.net)
  // improved by: Martijn Wieringa
  // improved by: jmweb
  //        note: Currently does not handle objects
  //   example 1: array_unshift(['van', 'Zonneveld'], 'Kevin');
  //   returns 1: 3

  var i = arguments.length;

  while (--i !== 0) {
    arguments[0].unshift(arguments[i]);
  }

  return arguments[0].length;
}  
  
//potential multi select (comma delimited list)
  var field = req.query.field || "undefined";
  var state = req.query.state || "undefined";  
  var county = req.query.county || "undefined";  
  var geonum = req.query.geonum || "undefined";  
  var geoid = req.query.geoid || "undefined";
  var table = req.query.table || "undefined";
  var sumlev = req.query.sumlev || "undefined";
  var zoom = req.query.zoom || 16;
  var bb = req.query.bb || "undefined";
  
var db = req.query.db || 'acs1115';
//set default for schema if it is missing
var schema = req.query.schema ||  function(){
  if(db==='acs1115' || db==='acs1014' || db==='acs0913' || db==='acs0812' || db==='c2010'){return 'data';}
  if(db==='c2000' || db==='c1990' || db==='c1980'){return 'sf1';}  
  return '';  //no valid database - will deal with later 
}();
  
  var port='5433';
  //if(db==='acs1014'){port='5433';} All databases now use 5433
  
  conString = "postgres://codemog:demography@gis.dola.colorado.gov:" + port + "/" + db;
  
  //carto or tiger or nhgis
var geo=""; //for now, geo will be set as a default

  if(db==='acs1115'){geo='carto';}
  if(db==='acs1014'){geo='carto';}
  if(db==='acs0913'){geo='carto';}
  if(db==='acs0812'){geo='carto';}
  if(db==='c2010'){geo='carto';}
  if(db==='c2000'){geo='carto';}
  if(db==='c1990'){geo='nhgis';}
  if(db==='c1980'){geo='nhgis';}

//carto or tiger or nhgis
var geodesc=""; //for now, geodesc will be set based upon sumlev
    
  if(sumlev=='160'){geodesc='place';}
  if(sumlev=='150'){geodesc='bg';}
  if(sumlev=='140'){geodesc='tract';}
  if(sumlev=='50'){geodesc='county';}
  if(sumlev=='40'){geodesc='state';}
  
  //by default limits to 1000 search results.  override by setting limit= in GET string
  var limit = parseInt(req.query.limit,10) || 1000;  
  
//if database is acs, check to see if moe option is flagged
var moe='no';
if(db=='acs0812' || db=='acs0913' || db=='acs1014' || db=='acs1115'){
  if (req.query.moe){
    moe=req.query.moe;
  }
} 

  //variables and arrays to use later
  var tablelist=[]; //array of all tables used in query
  var jointablelist=""; //working string of tables to be inserted into sql query
  var joinlist="";  //working string of 'where' condition to be inserted into sql query
  var arr=[];
  var arr2=[];
  var moefields=[]; //moe field array
  var tcolumns=[]; //columns gathered from table(s?)
  var ttlfields = [];
  
  var tolerance=0;  //for simplifying geometry

if(zoom==2){tolerance=0.2;}
if(zoom==3){tolerance=0.1;}
if(zoom==4){tolerance=0.07;}
if(zoom==5){tolerance=0.04;}
if(zoom==6){tolerance=0.018;}
if(zoom==7){tolerance=0.01;}
if(zoom==8){tolerance=0.005;}
if(zoom==9){tolerance=0.003;}
if(zoom==10){tolerance=0.0015;}
if(zoom==11){tolerance=0.001;}
if(zoom==12){tolerance=0.0005;}
if(zoom==13){tolerance=0.00025;}
if(zoom==14){tolerance=0.0001;}
if(zoom==15){tolerance=0.0001;}
if(zoom==16){tolerance=0.0001;}
  
  
  //if no fields are selected (then a table must be).  Create fields list based on the given table.
  
  //CRITICAL - FOR FASTEST PERFORMANCE, explicitly name fields.  
  //Otherwise, you must go through this loop to look up fields by querying database.
  //OPTION: perhaps conditionally load table-field lookup server-side
  //table lookup adds around 250ms to 300ms extra
if (field === "undefined"){
  
if (table !== "undefined"){
    
  var atablearray=explode(",", table);
  var atablestr="";
  
  for(var i=0; i<atablearray.length;i++){
    atablestr = atablestr + " table_name='" + atablearray[i] + "' or";
  }
  
    //trim last trailing 'or'
  atablestr=substr(atablestr,0,-2);
  
  //STOP: Don't Query DB for this!!! Too Slow!!!
  var tablesql="SELECT column_name from information_schema.columns where (" + atablestr + ") and table_schema='" + schema + "';";

  field = sendinternal(tablesql);  //ASYNC
  


} //end if table
} //end if field defined
  

  
    function check() {
        if (field === undefined || field ==="undefined") {
          setTimeout(check, 50); 
          //console.log('waiting...');
        }else{
          //console.log('field: '+field);
          continue_program();
        }
    }

    check();
      
  
  function continue_program(){
    
        function onlyUnique(value, index, self) { 
        return self.indexOf(value) === index;
      }
    
  // we have fields: either hand entered or derived from tables

    
  //break the comma delimited records from field into an array  
   ttlfields=explode(",", field);
    
    
   //if moe is set to yes, add the moe version of each field (push into new array, then merge with existing)
  if(moe==='yes'){

    var pos;
    
    for(var k=0;k<ttlfields.length;k++){
      //if text _moe doesn't already occur in the field name
      pos=strpos(ttlfields[k],'_moe');
      if(pos === false){
        moefields.push(substr_replace(ttlfields[k], '_moe', -3, 0));
      }
    }

    ttlfields = array_merge(ttlfields, moefields);

    ttlfields = ttlfields.filter( onlyUnique );
    
  //send moe modified field list back to main field list
  field = implode(',', ttlfields);
  }
    
    
//get a list of tables based upon characters in each field name  (convention: last 3 characters identify field number, previous characters are table name) 
    for(var m=0;m<ttlfields.length;m++){
      tablelist.push(substr(ttlfields[m],0,-3));
    }

  //remove duplicate tables in array
    tablelist = tablelist.filter( onlyUnique );
    
  //create a string to add to sql statement
    for(var n=0;n<tablelist.length;n++){
      jointablelist=jointablelist + " natural join " + schema + "." + tablelist[n];
    }


    //CASE 1:  you have a geonum
//essentially you don't care about anything else.  just get the data for that/those geonum(s)
if (geonum!=="undefined"){

    //break the comma delimited records from geonum into an array  
  var geonumarray=explode(",", geonum);

  //quick sidestep, calculate number of digits in geonum to assign join table geography
  //because maybe sumlev wasn't given
  if(geonumarray[0].length===3){geodesc='state';}
  if(geonumarray[0].length===6){geodesc='county';}
  if(geonumarray[0].length===8){geodesc='place';}
  if(geonumarray[0].length===12){geodesc='tract';}
  if(geonumarray[0].length===13){geodesc='bg';}  
  
//iterate through all geonum's
  for(var z=0;z<geonumarray.length;z++){
    joinlist=joinlist + " geonum=" + geonumarray[z] + " or";
  }
  
  //trim last trailing 'or'
  joinlist=substr(joinlist,0,-2);
  
//END CASE 1
} else if (geoid !== "undefined") {
//CASE 2:  you have a geoid
  
      //break the comma delimited records from geonum into an array  
  var geoidarray=explode(",", geoid);
  
      //quick sidestep, calculate number of digits in geoid to assign join table geography
  if(geoidarray[0].length===2){geodesc='state';}
  if(geoidarray[0].length===5){geodesc='county';}
  if(geoidarray[0].length===7){geodesc='place';}
  if(geoidarray[0].length===11){geodesc='tract';}
  if(geoidarray[0].length===12){geodesc='bg';}    
  
//iterate through all geoids, simply put a '1' in front and treat them like geonums
  for(var y=0;y<geoidarray.length;y++){
    joinlist=joinlist + " geonum=1" + geoidarray[y] + " or";
  }  

  //trim last trailing 'or'
  joinlist=substr(joinlist,0,-2);
 
//END CASE 2  
} else if (county!=="undefined" || state!=="undefined"){
  //CASE 3 - query
  
  var condition=""; //condition is going to be a 3 character string which identifies sumlev, county, state (yes/no) (1,0)
  if(county!=="undefined"){condition = "1";}else{condition = "0";}
  if(state!=="undefined"){condition = condition + "1";}else{condition = condition + "0";}

   
  if(county!=="undefined"){
//create county array out of delimited list
    var countylist="";
  
    //break the comma delimited records from county into an array  
  var countyarray=explode(",", county);
  
//iterate through all counties
  for(var x=0;x<countyarray.length;x++){
    countylist=countylist + " county=" + countyarray[x] + " or";
  }
    
  //trim last trailing 'or'
  countylist=substr(countylist,0,-2);
  }
  
  
   if(state!=="undefined"){
//create state array out of delimited list
       var statelist="";
  
    //break the comma delimited records from county into an array  
  var statearray=explode(",", state);
  
//iterate through all states
     for(var u=0;u<statearray.length;u++){
       statelist=statelist + " state=" + statearray[u] + " or";
     }
  
  //trim last trailing 'or'
  statelist=substr(statelist,0,-2);
   }
  
  
  //every possible combination of county, state
  if(condition==='01'){joinlist = " (" + statelist + ") ";}
  if(condition==='11'){joinlist = " (" + countylist + ") and (" + statelist + ") ";}
  if(condition==='10'){joinlist = " (" + countylist + ") ";}
  //END CASE 3
} else if(sumlev!=="undefined"){
  //CASE 4: Only Sumlev
  joinlist = " 5=5 "; //nonsense here because of preceding 'AND'
  //END CASE 4
}else{
  // CASE 5: No Geo
  console.log('error: case 5');
  return;
//END CASE 5
}

    

var bbstr=""; //bounding box string

//potential single select
if (bb!=="undefined"){
bbstr=geo + "." + geodesc + ".geom && ST_MakeEnvelope(" + bb + ", 4326) and ";
}  //bounding box example: "-105,40,-104,39" no spaces no quotes

  //CONSTRUCT MAIN SQL STATEMENT
// execute query
var sql = "SELECT geoname, geonum, " + field + ", st_asgeojson(st_transform(ST_Simplify((\"geom\")," + tolerance + "),4326),4) AS geojson from " + geo + "." + geodesc + " " + jointablelist + " where " + bbstr + " " + joinlist + " limit " + limit + ";";
    
    console.log(sql);
  
 sendtodatabase(sql);  
  
    
}


    function sendtodatabase(sqlstring) {

        var client = new pg.Client(conString);

        client.connect(function(err) {

            if (err) {
                return console.error('could not connect to postgres', err);
            }

            client.query(sqlstring, function(err, result) {

                if (err) {
                    return console.error('error running query', err);
                }
              
              var resultdata=result.rows;

              
              // Build GeoJSON
var output    = '';
var rowOutput = '';
              
              for(var t=0;t<resultdata.length;t++){

    rowOutput = (rowOutput.length > 0 ? ',' : '') + '{"type": "Feature", "geometry": ' + resultdata[t]['geojson'] + ', "properties": {';
    var props = '';
    var id    = '';

    
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
    }

     function sendinternal(sqlstring) {

        var client = new pg.Client(conString);

        client.connect(function(err) {

            if (err) {
                return console.error('could not connect to postgres', err);
            }

            client.query(sqlstring, function(err, result) {

                if (err) {
                    return console.error('error running query', err);
                }

                client.end();

              var tableresult = (result.rows);
              
              

              for(var i=0;i<tableresult.length;i++){
                if(tableresult[i].column_name!=='geonum'){
                  tcolumns.push(tableresult[i].column_name);
                  }
                }
              
                field = implode(',', tcolumns); //$field becomes fields queried from info schema based upon table

                return field;
 

              
            });
        });
    } 
  
});


}
