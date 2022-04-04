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
  // console.log("docs.len", races.length);
  return races;
};

const raw_race_runner = async (races) => {
  try {
    races = cyclic_depedency.struct_race_row_data(races);
    races = _.groupBy(races, "raceid");
    console.log("found", _.keys(races).length);
    let data = [];
    for (let [rid, ar] of _.entries(races)) {
      // console.log(rid, ar.length);
      if (_.isEmpty(ar)) continue;
      let date = ar[0].date;
      let dist = ar[0].distance;
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
      let f_12 = f_ob[12];

      let eg_1_2 = (g_1_2 * 1000) / dist;
      let eg_11_12 = (g_11_12 * 1000) / dist;

      let ob = {
        rid,
        hid_1,
        hid_12,
        g_1_2,
        g_11_12,
        eg_1_2,
        eg_11_12,
        f_1,
        f_12,
        date,
        dist,
      };
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
    console.log("gap_leader wrote", bulk.length);
  } catch (err) {
    console.log("raw_race_runner\n", err);
  }
};

const run_duration = async (st, ed) => {
  st = new Date(st).toISOString();
  ed = new Date(ed).toISOString();
  let races = await download_races(st, ed);
  await raw_race_runner(races);
};

const main = async (st, ed) => {
  let now = new Date(st).getTime();
  let end = new Date(ed).getTime();
  console.log(st, ed);
  console.log(now, end);
  let offset = 1000 * 60 * 30;
  while (now < end) {
    let now_ed = Math.min(end, now + offset);
    await run_duration(now, now_ed);
    now += offset;
  }
};

let limit = 500;
const get_gap_horses = async ({
  limit = 100,
  query,
  projection,
  sort,
  struct_doc,
}) => {
  try {
    let st = moment().subtract(5, "days").toISOString().slice(0, 10);
    let docs = await zed_db.db
      .collection("gap_leader")
      .find({ ...query, date: { $gt: st } }, { projection })
      .sort(sort)
      .limit(limit)
      .toArray();
    docs = docs.map(struct_doc);
    docs = _.compact(docs);
    let hids = _.map(docs, "hid");
    let hdocs = await zed_db.db
      .collection("horse_details")
      .find({ hid: { $in: hids } }, { projection: { hid: 1, name: 1, tc: 1 } })
      .toArray();
    hdocs = _.chain(hdocs).keyBy("hid").value();
    docs = docs.map((d) => {
      return { ...d, ...hdocs[d.hid] };
    });
    return docs;
  } catch (err) {
    console.log("err", err);
    return [];
  }
};

const create_leaderboard = async (ldate) => {
  ldate = new Date(ldate).toISOString().slice(0, 10);
  let date_ed = moment(new Date(ldate).toUTCString())
    .add(1, "day")
    .toISOString();
  console.log({ ldate });
  console.log({ date_ed });
  let hids_wins = await get_gap_horses({
    limit,
    query: {
      f_1: 1,
      g_1_2: { $ne: null },
      date: { $lte: date_ed },
    },
    projection: {
      hid_1: 1,
    },
    sort: { g_1_2: -1 },
    struct_doc: (doc) => ({ hid: doc.hid_1 }),
  });
  hids_wins = _.map(hids_wins, "hid");
  let id_w = `gapl-winners-${ldate}`;
  await zed_db.db
    .collection("gap_leader")
    .updateOne(
      { id: id_w },
      { $set: { id: id_w, type: "leader", hids: hids_wins } },
      { upsert: true }
    );
  console.log(hids_wins);
  console.log("done", id_w);

  let hids_loses = await get_gap_horses({
    limit,
    query: {
      f_12: 1,
      g_11_12: { $ne: null },
      date: { $lte: date_ed },
    },
    projection: {
      hid_12: 1,
    },
    sort: { g_11_12: -1 },
    struct_doc: (doc) => ({ hid: doc.hid_12 }),
  });
  hids_loses = _.map(hids_loses, "hid");
  let id_l = `gapl-losers-${ldate}`;
  await zed_db.db
    .collection("gap_leader")
    .updateOne(
      { id: id_l },
      { $set: { id: id_l, type: "leader", hids: hids_loses } },
      { upsert: true }
    );
  console.log("done", id_l);
};

const test = async () => {
  let date = "2022-01-07";
  await create_leaderboard(date);
};

const max_gap = { main, create_leaderboard, test, raw_race_runner };
module.exports = max_gap;
