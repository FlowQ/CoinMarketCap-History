//packages de dev'
var fs = require('fs');
var moment = require('moment'); //http://momentjs.com/docs/
var colors = require('colors');
var request = require('request');
var sql = require('mssql');
console.log(("STARTING - " + new Date()).yellow);
//configuration


var config = {
    user: 'python',
    password: 'python123',
    server: 'localhost\\SQLSERVER12', // You can use 'localhost\\instance' to connect to named instance
    database: 'crypto',
};

function getData() {

    insert_group_id = moment().format('x');
  // store the data
    sql.connect(config).then(function() {
      new sql.Request().query('select count(*) from Moneys').then(function(recordset) {
          console.dir(recordset);
      }).catch(function(err) {
        console.log("SELECT COUNT ERROR".red);
        console.log(err);
      });
      var sql_table = new sql.Table('Moneys');
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


      // get the data
      request('https://api.coinmarketcap.com/v1/ticker/', function (error, response, body) {
        body_parsed = JSON.parse(body);
        for(var i in body_parsed) {
          sql_table.rows.add(
            body_parsed[i]['id'],
            body_parsed[i]['name'],
            body_parsed[i]['symbol'],
            parseInt(body_parsed[i]['rank']),
            parseFloat(body_parsed[i]['price_usd']),
            parseFloat(body_parsed[i]['price_btc']),
            parseFloat(body_parsed[i]['24h_volume_usd']),
            parseFloat(body_parsed[i]['market_cap_usd']),
            parseFloat(body_parsed[i]['available_supply']),
            parseFloat(body_parsed[i]['total_supply']),
            parseFloat(body_parsed[i]['percent_change_1h']),
            parseFloat(body_parsed[i]['percent_change_24h']),
            parseFloat(body_parsed[i]['percent_change_7d']),
            parseInt(body_parsed[i]['last_updated']),
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
              //sql.close();
            }

        });
      });
    }).catch(function(err) {
      console.log("CONNECTION ERROR".red);
      console.log(err);
    });
}
setInterval(function(){
  getData();
  console.log("get data");
}, 5 * 1000);


  // create table query
      // CREATE TABLE [dbo].[Moneys](
      // 	[ID] [nchar](300) NULL,
      // 	[Name] [nchar](300) NULL,
      // 	[Symbol] [nchar](100) NULL,
      // 	[Rank] [int] NULL,
      // 	[Price_USD] [numeric](30, 8) NULL,
      // 	[Price_BTC] [numeric](30, 8) NULL,
      // 	[24H_volume_USD] [numeric](30, 8) NULL,
      // 	[Market_Cap_USD] [numeric](30, 8) NULL,
      // 	[Available_Supply] [bigint] NULL,
      // 	[Total_Supply] [bigint] NULL,
      // 	[Percent_Change_1H] [numeric](30, 8) NULL,
      // 	[Percent_Change_24H] [numeric](30, 8) NULL,
      // 	[Percent_Change_7D] [numeric](30, 8) NULL,
      // 	[Last_Update] [bigint] NULL,
      // 	[INSERT_GROUP_ID] [int] NULL,
      // 	[INSERT_ID] [int] IDENTITY(1,1) NOT NULL
      // ) ON [PRIMARY]
