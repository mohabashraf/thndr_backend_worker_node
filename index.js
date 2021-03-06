const keys = require("./keys");
const redis = require("redis");

const { Pool } = require("pg");
const pgClient = new Pool({
  user: keys.pgUser,
  host: keys.pgHost,
  database: keys.pgDatabase,
  password: keys.pgPassword,
  port: keys.pgPort,
});

pgClient.on("connect", (client) => {
  client
    .query(
      "CREATE TABLE IF NOT EXISTS stock_analysis(id SERIAL PRIMARY KEY, stock_id INT, technical_analysis_id INT, target_hit Boolean)"
    )
    .catch((err) => console.error(err));

  client
    .query(
      "CREATE TABLE IF NOT EXISTS stocks(id SERIAL PRIMARY KEY, stock_id text, name text, price INT, availability INT, timestamp timestamp)"
    )
    .catch((err) => console.error(err));

    client
    .query(
      "CREATE TABLE IF NOT EXISTS admin_technical_analysis(id SERIAL PRIMARY KEY, stock_id text, target text, type text, time timestamp)"
    )
    .catch((err) => console.error(err));
});

const redisClient = redis.createClient({
  host: keys.redisHost,
  port: keys.redisPort,
  retry_strategy: () => 1000,
});
const sub = redisClient.duplicate();

sub.on("message", async (channel, message) => {
  const technical_analysis = await new Promise((resolve) => {
    redisClient.hget("technical_analysis", message, (err, value) => {
      if (err) {
        reject(err);
      }
      if (value) {
        resolve(value);
      }
      resolve("");
    });
  }).catch((err) => {
    console.log("Errot", err);
  });
  const stock = await new Promise((resolve) => {
    redisClient.hget("stocks", message, (err, value) => {
      if (err) {
        reject(err);
      }
      if (value) {
        resolve(value);
      }
    });
  }).catch((err) => {
    console.log("Errot", err);
  });


  if (technical_analysis !== "" && stock) {


    const tech = JSON.parse(technical_analysis);
    const stockData = JSON.parse(stock);
    if (stockData.price > tech.target && tech.type === "UP") {
      tech.target_hit = true;
    } else {
      tech.target_hit = false;
    }
    stockData.technical_analysis = tech;

    const stock_psql = await pgClient.query(
      "INSERT INTO stocks(stock_id, name, price, availability, timestamp) VALUES($1, $2, $3, $4, $5) RETURNING id",
      [
        stockData.stock_id,
        stockData.name,
        stockData.price,
        stockData.availability,
        stockData.timestamp,
      ]
    );
    const analysis_psql = await pgClient.query(
      "INSERT INTO admin_technical_analysis(stock_id, target, type, time) VALUES($1, $2, $3, $4) RETURNING id",
      [stockData.stock_id, tech.target, tech.type, new Date().toISOString()]
    );


    await pgClient.query(
      "INSERT INTO stock_analysis( stock_id, technical_analysis_id, target_hit) VALUES($1, $2, $3)",
      [stock_psql.rows[0].id, analysis_psql.rows[0].id, tech.target_hit]
    );


    // const record = await pgClient.query(
    //   `SELECT  * From admin_technical_analysis where stock_id = ${stockData.stock_id} ORDER BY time DESC LIMIT 1`,
    // );

    // console.log("The record is " + record.rows)


    redisClient.hset("stocks_analysis", message, JSON.stringify(stockData));
  } else if (stock) {

    const stockData = JSON.parse(stock);
    const stock_psql = await pgClient.query(
      "INSERT INTO stocks(stock_id, name, price, availability, timestamp) VALUES($1, $2, $3, $4, $5) RETURNING id",
      [
        stock.stock_id,
        stock.name,
        stock.price,
        stock.availability,
        stock.timestamp,
      ]
    );



    redisClient.hset("stocks_analysis", message, JSON.stringify(stockData));
  }
});
sub.subscribe("insert");
