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
  console.log("docs.len", races.length);
  return races;
};

const run_duration = async (st, ed) => {
  st = new Date(st).toISOString();
  ed = new Date(ed).toISOString();
  let races = await download_races(st, ed);
  races = _.groupBy(races, "raceid");
  console.log("found", _.keys(races).length);
  let data = [];
  for (let [rid, ar] of _.entries(races)) {
    console.log(rid, ar.length);
    if (_.isEmpty(ar)) continue;
    let date = ar[0].date;
    let f_ob = _.chain(ar).keyBy("place").mapValues("flame").value();
    let hid_ob = _.chain(ar).keyBy("place").mapValues("hid").value();
    let t_ob = _.chain(ar).keyBy("place").mapValues("finishtime").value();
    // console.log(f_ob);
    // console.log(hid_ob);
    // console.log(t_ob);
    let g_1_2 = t_ob[2] - t_ob[1];
    let g_11_12 = t_ob[12] - t_ob[11];
    let hid_1 = hid_ob[1];
    let hid_12 = hid_ob[12];
    let f_1 = f_ob[1];
    let f_12 = f_ob[1];
    let ob = { rid, hid_1, hid_12, g_1_2, g_11_12, f_1, f_12, date };
    data.push(ob);
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
  let now = new Date(st).getTime();
  let end = new Date(ed).getTime();
  console.log(st, ed);
  console.log(now, end);
  let offset = 1000 * 60 * 15;
  while (now < end) {
    let now_ed = Math.min(end, now + offset);
    await run_duration(now, now_ed);
    now += offset;
  }
};

const max_gap = { main };
module.exports = max_gap;
