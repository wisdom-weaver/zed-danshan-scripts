const Redis = require("ioredis");
const { jparse, jstr } = require("../utils/cyclic_dependency");
const _ = require("lodash");
const rurl = "127.0.0.1:6379";
const redis = new Redis(rurl);

const rset = async (redid, val, ex) => {
  await redis.setex(redid, ex, jstr(val));
  return;
};
const rget = async (redid) => {
  let val = await redis.get(redid);
  return jparse(val);
};

const rfn = async (redid, fn, ex, p = 0) => {
  let val = await redis.get(redid);
  val = jparse(val);
  if (p) console.log(redid, _.isNil(val));
  if (!val) {
    val = await fn();
    await redis.setex(redid, ex, jstr(val));
  }
  return val;
};

const red = { redis, rfn, rget, rset };
module.exports = red;
