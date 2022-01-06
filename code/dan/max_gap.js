const moment = require("moment");
const { zed_ch, zed_db } = require("../connection/mongo_connect");
const utils = require("../utils/utils");
const _ = require("lodash");
const cyclic_depedency = require("../utils/cyclic_dependency");

const download_races = async (st, ed) => {
  console.log(st, "->", ed);
  let races = await zed_ch.db
    .collection("zed")
    .find({ 2: { $gte: st, $lte: ed } })
    .toArray();
  races = cyclic_depedency.struct_race_row_data(races);
  return races;
};

const run_duration = async (st, ed) => {
  let races = await download_races(st, ed);
  races = _.keyBy(races, "raceid");
  console.log("found", _.keys(races).length);
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
        update: { $set: ob },
        upsert: true,
      },
    });
  }
  if (!_.isEmpty(bulk))
    await zed_db.db.collection("gap_leader").bulkWrite(bulk);
  console.log("wrote", bulk.length);
};

const main = async (st, ed) => {
  let now = Date.now(st);
  let end = Date.now(ed);
  let offset = 1000 * 60 * 15;
  while (now < end) {
    let now_ed = Math.min(end, now + offset);
    await run_duration(now, now_ed);
    now += offset;
  }
};

const max_gap = { main };
module.exports = max_gap;
