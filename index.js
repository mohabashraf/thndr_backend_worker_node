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
    .catch((err) => console.error(err));});


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

  if (technical_analysis && stock) {
    const tech = JSON.parse(technical_analysis);
    const stockData = JSON.parse(stock);
    if (stockData.price > tech.target && tech.type === "UP") {
      tech.target_hit = true;
    } else {
      tech.target_hit = false;
    }
    stockData.technical_analysis = tech;


    const stock_psql = pgClient.query(
      "INSERT INTO stocks(stock_id, name, price, availability, timestamp timestamp) VALUES($1, $2, $3, $4)",
      [stock.stock_id, stock.name, stock.price, stock.availability, stock.timestamp]
    );

    console.log("The stock from psql" + stock_psql);
    
    redisClient.hset("stocks_analysis", message, JSON.stringify(stockData));
  }
});
sub.subscribe("insert");
