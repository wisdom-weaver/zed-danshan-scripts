const moment = require("moment");
const { zed_ch, zed_db } = require("../connection/mongo_connect");
const utils = require("../utils/utils");
const _ = require("lodash");
const cyclic_depedency = require("../utils/cyclic_dependency");

const download_races = async (date) => {
  let st = moment(date).subtract(15, "minutes").toISOString();
  let ed = moment(date).toISOString();
  console.log(st, "->", ed);
  let races = await zed_ch.db
    .collection("zed")
    .find({ 2: { $gte: st, $lte: ed } })
    .toArray();
  console.log("races.len", races.length);
  races = cyclic_depedency.struct_race_row_data(races);
  return races;
};

const main = async () => {
  let date = "2021-12-01";
  let races = await download_races(date);
  console.log(races.length);
  races = _.keyBy(races, "raceid");
  let data = [];
  for (let [rid, ar] of _.entries(races)) {
    let f_ob = _.chain(ar).keyBy("place").mapValues("flame").value();
    let hid_ob = _.chain(ar).keyBy("place").mapValues("hid").value();
    let t_ob = _.chain(ar).keyBy("place").mapValues("finishtime").value();
    let g_1_2 = t_ob[2] - t_ob[1];
    let g_11_12 = t_ob[12] - t_ob[11];
    let hid_1 = hid_ob[1];
    let hid_12 = hid_ob[12];
    let f_1 = f_ob[1];
    let f_12 = f_ob[1];
    data.push({ rid, hid_1, hid_12, g_1_2, g_11_12, f_1, f_12 });
  }
  let bulk = [];
  for (let ob of data) {
    if (_.isEmpty(ob)) continue;
    bulk.push({
      updateOne: {
        filter: { rid: ob.rid },
        update: ob,
        upsert: true,
      },
    });
  }
  if (!_.isEmpty(bulk))
    await zed_db.db.collection("gap_leader").bulkWrite(bulk);
  console.log("wrote", bulk.length);
};

const max_gap = { main };
module.exports = max_gap;
