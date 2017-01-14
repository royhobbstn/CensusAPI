// test /routes/demog.js

var assert = require('assert');
var helpers = require('../helpers/helpers.js');




describe('Helpers', function () {


    describe('#pad()', function () {
        it('should return "08" when receiving (8,2,"0")', function () {
            assert.equal(helpers.pad(8, 2, "0"), "08");
        });
        it('should return "31" when receiving (31,2,"0")', function () {
            assert.equal(helpers.pad(31, 2, "0"), "31");
        });
        it('should return "101" when receiving (101,3,"0")', function () {
            assert.equal(helpers.pad(101, 3, "0"), "101");
        });
        it('should return "005" when receiving (5,3,"0")', function () {
            assert.equal(helpers.pad(5, 3, "0"), "005");
        });
        it('should return "00692" when receiving (692,5,"0")', function () {
            assert.equal(helpers.pad(692, 5, "0"), "00692");
        });
    });

    describe('#sqlList()', function () {
        it('should return a SQL "or" list of fields when receiving an array', function () {
            assert.equal(helpers.sqlList(["b01002001", "b01002002", "b01002003", "b19013001", "b01002_moe001", "b01002_moe002", "b01002_moe003", "b19013_moe001"], "column_id"),
                " column_id='b01002001' or column_id='b01002002' or column_id='b01002003' or column_id='b19013001' or column_id='b01002_moe001' or column_id='b01002_moe002' or column_id='b01002_moe003' or column_id='b19013_moe001' ");
        });
        it('should return a SQL "or" list of fields when receiving a comma delimited string', function () {
            assert.equal(helpers.sqlList("b01002001,b01002002,b01002003,b19013001,b01002_moe001,b01002_moe002,b01002_moe003,b19013_moe001", "column_id"),
                " column_id='b01002001' or column_id='b01002002' or column_id='b01002003' or column_id='b19013001' or column_id='b01002_moe001' or column_id='b01002_moe002' or column_id='b01002_moe003' or column_id='b19013_moe001' ");
        });
    });



});
