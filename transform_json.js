var fs = require('fs');
var colors = require('colors');
var moment = require('moment');
var sql = require('mssql');
var config = {
    user: 'python',
    password: 'python123',
    server: 'localhost\\SQLSERVER12', // You can use 'localhost\\instance' to connect to named instance
    database: 'crypto',
}
var testFile = require('./testFile');

insert_group_id = moment().format('x');

sql.connect(config).then(function() {
  //teste que le connecteur fonctionne
  new sql.Request().query('select count(*) from CMKhistory').then(function(recordset) {
      console.dir(recordset);
  }).catch(function(err) {
    console.log("SELECT COUNT ERROR".red);
    console.log(err);
  });
  var sql_table = new sql.Table('CMKhistory');
  sql_table.columns.add('ID', sql.VarChar(300), {nullable: true});
  sql_table.columns.add('Name', sql.VarChar(300), {nullable: true});
  sql_table.columns.add('Symbol', sql.VarChar(100), {nullable: true});
  sql_table.columns.add('Rank', sql.Int, {nullable: true});
  sql_table.columns.add('Price_USD', sql.Numeric(30,8), {nullable: true});
  sql_table.columns.add('Price_BTC', sql.Numeric(30,8), {nullable: true});
  sql_table.columns.add('24H_volume_USD', sql.Numeric(30,8), {nullable: true});
  sql_table.columns.add('Market_Cap_USD', sql.Numeric(30,8), {nullable: true});
  sql_table.columns.add('Available_Supply', sql.BigInt, {nullable: true});
  sql_table.columns.add('Total_Supply', sql.BigInt, {nullable: true});
  sql_table.columns.add('Percent_Change_1H', sql.Numeric(30,8), {nullable: true});
  sql_table.columns.add('Percent_Change_24H', sql.Numeric(30,8), {nullable: true});
  sql_table.columns.add('Percent_Change_7D', sql.Numeric(30,8), {nullable: true});
  sql_table.columns.add('Last_Update', sql.BigInt, {nullable: true});
  sql_table.columns.add('INSERT_GROUP_ID', sql.BigInt, {nullable: true});
  var sql_table_backup = sql_table.rows;

  for(i in testFile) {
      sql_table.rows.add(
        testFile[i]['id'],
        testFile[i]['name'],
        testFile[i]['symbol'],
        parseInt(testFile[i]['rank']),
        parseFloat(testFile[i]['price_usd']),
        parseFloat(testFile[i]['price_btc']),
        parseFloat(testFile[i]['24h_volume_usd']),
        parseFloat(testFile[i]['market_cap_usd']),
        parseFloat(testFile[i]['available_supply']),
        parseFloat(testFile[i]['total_supply']),
        parseFloat(testFile[i]['percent_change_1h']),
        parseFloat(testFile[i]['percent_change_24h']),
        parseFloat(testFile[i]['percent_change_7d']),
        parseInt(testFile[i]['last_updated']),
        insert_group_id
      )
    }
    //sql_table.rows.add();
    var request = new sql.Request();
    request.bulk(sql_table, function(err, rowCount) {
        if(err) console.log("BULK INSERT ERROR ".red, err) ;
        else {
          console.log("BULK INSERT - ".green, rowCount);
          console.log(("Done " + new Date()).green);
          sql_table.rows = sql_table_backup;
        }

    });
}).catch(function(err) {
  console.log("CONNECTION ERROR".red);
  console.log(err);
});
// Pour lire des larges fichiers
// var fs = require('fs');
// var readline = require('readline');
// var stream = require('stream');
//
// var instream = fs.createReadStream('historics_json.json');
// var outstream = new stream;
// var rl = readline.createInterface(instream, outstream);
//
// rl.on('line', function(line) {
//   // process line here
//   console.log(line);
// });
//
// rl.on('close', function() {
//   // do something on finish here
//   console.log("close");
// });
//
