//MICROSERVICE for 

//returns:
/*
http://red-meteor-147235.nitrousapp.com:4000/advsearch?advsumlev=50&advstate=6&advsign=gt&advtext=20000&advtable=b19013&advnumerator=fp.b19013001&advdenominator=1


*/
module.exports = function(app, pg, conString){

app.get('/meta', function(req, res) {

  //potential multi select (comma delimited list)
  var db = req.query.db || "undefined";
  var schema = req.query.schema || "undefined";  

  //Query metadata
  var tblsql="SELECT table_id, table_title, universe from " + schema + ".census_table_metadata;";


    sendtodatabase(tblsql);


    function sendtodatabase(sqlstring) {

        var client = new pg.Client(conString+db);

        client.connect(function(err) {

            if (err) {
                return console.error('could not connect to postgres', err);
            }

            client.query(sqlstring, function(err, result) {

                if (err) {
                    return console.error('error running query', err);
                }

              
              
              var tableresult=result.rows;
              
              var tblarrfull=[];
              var tblarr={};
              
              for(var i=0;i<tableresult.length;i++){
                tblarr={};
                tblarr.table_id = tableresult[i].table_id;
                tblarr.table_title = tableresult[i].table_title;
                tblarr.universe = tableresult[i].universe;
                tblarrfull.push(tblarr);
              }
              
             
              
                res.set({
                    "Content-Type": "application/json"
                });
                res.send(JSON.stringify(tblarrfull));

              
              

                client.end();

            });
        });
    }

});

}