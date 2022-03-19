const _ = require("lodash");
const { zed_db } = require("../../connection/mongo_connect");

const st_date = "2022-03-17T15:00:00Z";
const v_code = 5;
let t = 0;
const get_compiler_hids = async () => {
  let docs = await zed_db.db
    .collection("horse_details")
    .find({ tx_date: { $gte: st_date } }, { hid: 1, _id: 0 })
    .toArray();
  let hids = _.map(docs, "hid");
  console.log("compiler_hids", hids);
  return hids;
};

const compiler_common = {
  st_date,
  get_compiler_hids,
  v_code,
  t,
};
module.exports = compiler_common;
