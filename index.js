const keys = require('./keys');
const redis = require('redis');

const redisClient = redis.createClient({
  host: keys.redisHost,
  port: keys.redisPort,
  retry_strategy: () => 1000,
});
const sub = redisClient.duplicate();


sub.on('message', async (channel, message) => {
  const technical_analysis = await new Promise((resolve) => {
    redisClient.hget("technical_analysis", message, (err, value) => {
      if (err) {
        reject(err);
      }
       if(value){
         resolve(JSON.parse(value.toString()));
       } 
    });
  }).catch((err) => {
    console.log("Errot", err);
  });

  if(technical_analysis){
    redisClient.hset('stocks_analysis', message, technical_analysis);

  }


});
sub.subscribe('insert');
