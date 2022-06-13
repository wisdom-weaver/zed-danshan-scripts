const _ = require("lodash");
const { zed_db } = require("../connection/mongo_connect");
const { redis } = require("../connection/redis");
const { jparse, jstr } = require("../utils/cyclic_dependency");
const ymca2_table = require("../v3/ymca2_table");
const ymca5_table = require("../v5/ymca5_table");

let data = {
  z_ALL: null,
  ymca2_avgs: null,
  ymca5_avgs: null,
};

const get_z_ALL_meds = async () => {
  let doc = await zed_db.db.collection("z_meds").findOne({ id: "z_ALL" });
  let ob = _.chain(doc.ar).keyBy("id").mapValues("med").value();
  console.log("GOT", "z_ALL");
  return ob;
};

const redis_gs = async (redid, ex, fn) => {
  let val = await redis.get(redid);
  if (!val) {
    val = await fn();
    await redis.setex(redid, ex, jstr(val));
  } else {
    console.log("got from redis", redid);
    val = jparse(val);
  }
  return val;
};

const download = async () => {
  // data.ymca2_avgs = await redis_gs("global_req.data.ymca2_avgs", 15 * 60, () =>
  //   ymca2_table.get()
  // );
  // data.ymca5_avgs = await redis_gs("global_req.data.ymca5_avgs", 15 * 60, () =>
  //   ymca5_table.get()
  // );
  // data.z_ALL = await redis_gs("global_req.data.z_ALL", 15 * 60, () =>
  //   get_z_ALL_meds()
  // );
  data.ymca2_avgs = await ymca2_table.get();
  data.ymca5_avgs = await ymca5_table.get();
  data.z_ALL = await get_z_ALL_meds();

};

const get_data = () => data;

const global_req = { data, download, get_data };

module.exports = global_req;
