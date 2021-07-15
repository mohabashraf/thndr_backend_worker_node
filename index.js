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
      resolve = ""
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
      [stock.stock_id, stock.name, stock.price, stock.availability, stock.timestamp]
    );

    he stock from psql1{"command":"INSERT","rowCount":1,"oid":0,"rows":[{"id":31}],"fields":[{"name":"id","tableID":16405,"columnID":1,"dataTypeID":23,"dataTypeSize":4,"dataTypeModifier":-1,"format":"text"}],"_parsers":[null],"_types":{"_types":{"arrayParser":{},"builtins":{"BOOL":16,"BYTEA":17,"CHAR":18,"INT8":20,"INT2":21,"INT4":23,"REGPROC":24,"TEXT":25,"OID":26,"TID":27,"XID":28,"CID":29,"JSON":114,"XML":142,"PG_NODE_TREE":194,"SMGR":210,"PATH":602,"POLYGON":604,"CIDR":650,"FLOAT4":700,"FLOAT8":701,"ABSTIME":702,"RELTIME":703,"TINTERVAL":704,"CIRCLE":718,"MACADDR8":774,"MONEY":790,"MACADDR":829,"INET":869,"ACLITEM":1033,"BPCHAR":1042,"VARCHAR":1043,"DATE":1082,"TIME":1083,"TIMESTAMP":1114,"TIMESTAMPTZ":1184,"INTERVAL":1186,"TIMETZ":1266,"BIT":1560,"VARBIT":1562,"NUMERIC":1700,"REFCURSOR":1790,"REGPROCEDURE":2202,"REGOPER":2203,"REGOPERATOR":2204,"REGCLASS":2205,"REGTYPE":2206,"UUID":2950,"TXID_SNAPSHOT":2970,"PG_LSN":3220,"PG_NDISTINCT":3361,"PG_DEPENDENCIES":3402,"TSVECTOR":3614,"TSQUERY":3615,"GTSVECTOR":3642,"REGCONFIG":3734,"REGDICTIONARY":3769,"JSONB":3802,"REGNAMESPACE":4089,"REGROLE":4096}},"text":{},"binary":{}},"RowCtor":null,"rowAsArray":false}

    console.log(stock_psql.rows[0].id);
    
    redisClient.hset("stocks_analysis", message, JSON.stringify(stockData));
  }else if (stock){
    const stockData = JSON.parse(stock);
    const stock_psql = await pgClient.query(
      "INSERT INTO stocks(stock_id, name, price, availability, timestamp) VALUES($1, $2, $3, $4, $5) RETURNING id",
      [stock.stock_id, stock.name, stock.price, stock.availability, stock.timestamp]
    );

    console.log("The stock from psql2" + stock_psql.rows);
    
    redisClient.hset("stocks_analysis", message, JSON.stringify(stockData));
  }
});
sub.subscribe("insert");
