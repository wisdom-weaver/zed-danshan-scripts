const Redis = require("ioredis");
const rurl = "127.0.0.1:6379";
const redis = new Redis(rurl);
module.exports = { redis };
