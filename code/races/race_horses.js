const moment = require("moment");
const _ = require("lodash");
const { zed_db } = require("../connection/mongo_connect");
const mega = require("../v3/mega");
const cron = require("node-cron");
const cron_parser = require("cron-parser");
const utils = require("../utils/utils");
const cyclic_depedency = require("../utils/cyclic_dependency");
const { getv, cdelay } = require("../utils/utils");
const { get_date_range_fromto } = require("../utils/cyclic_dependency");
const { delay } = require("lodash");

const def_cs = 4000;
const run_cs = 30;

const coll = "stats_check";
const push = async (ob) => {
  let { hid, date, rid, tc } = ob;
  // let doc = await zed_db.db.collection(coll).findOne({ hid });
  // console.log(doc);
  let resp = await zed_db.db.collection(coll).updateOne(
    { hid },
    {
      $setOnInsert: { hid, last_updated: null },
      $addToSet: { races: { rid, date, tc } },
    },
    { upsert: true }
  );
  return 1;
};

const push_ar = async (ar) => {
  console.table(ar);
  await Promise.all(ar.map(push));
};

const agglag = [
  {
    $project: {
      hid: 1,
      races: 1,
      last_updated: 1,
      sizegt0: {
        $gt: [{ $size: "$races" }, 0],
      },
    },
  },
  {
    $match: {
      sizegt0: true,
    },
  },
  {
    $project: {
      hid: 1,
      latest_race: {
        $ifNull: [{ $max: "$races.date" }, "2000"],
      },
      last_updated: 1,
    },
  },
  {
    $project: {
      hid: 1,
      last_updated: 1,
      latest_race: 1,
      elig: {
        $or: [
          { $lt: ["$last_updated", "$latest_race"] },
          { $eq: ["$last_updated", null] },
        ],
      },
    },
  },
  {
    $match: {
      elig: true,
    },
  },
  // {
  //   $count: "hid",
  // },
];

const update_lagging = async () => {
  let ref = zed_db.db.collection(coll);
  let resp = await ref
    .aggregate([...agglag, { $sort: { latest_race: -1 } }, { $limit: 100 }])
    .toArray();
  if (_.isEmpty(resp)) return console.log("no lagging horses");
  console.table(resp);
  let hids = _.map(resp, "hid");
  for (let chu of _.chunk(hids, run_cs)) {
    await mega.only(chu, run_cs);
  }
};

const update_dur = async ([st, ed]) => {
  console.log({ st, ed });
  let ref = zed_db.db.collection(coll);
  let resp = await ref
    .aggregate([...agglag, { $match: { latest_race: { $gte: st, $lte: ed } } }])
    .toArray();
  if (_.isEmpty(resp)) return console.log("no lagging horses");
  console.table(resp);
  let hids = _.map(resp, "hid");
  for (let chu of _.chunk(hids, run_cs)) {
    await mega.only(chu, run_cs);
  }
};

const remain = async () => {
  let ref = zed_db.db.collection(coll);
  while (true) {
    let c = await ref
      .aggregate([...agglag, { $sort: { latest_race: -1 } }, { $count: "hid" }])
      .toArray();
    c = getv(c, "0.hid") ?? 0;
    console.log("horses need stats : ", c);
    await cdelay(5000);
  }
};

const update_horse_tc = async (doc) => {
  let { hid, tc } = doc || {};
  if (tc === undefined || (tc === null && ![0, 1, 2, 3, 4, 5, 6].includes(tc)))
    return;
  await zed_db.db
    .collection("horse_details")
    .updateOne({ hid }, { $set: { tc } });
};

let lagrunning = 0;
let lagrunning2 = 0;
const run_cron = () => {
  let cron_str = "*/5 * * * * *";
  cyclic_depedency.print_cron_details(cron_str);
  const runner = async () => {
    if (lagrunning == 1) return console.log("race horses update running");
    try {
      lagrunning = 1;
      await update_lagging();
      lagrunning = 0;
    } catch (err) {
      lagrunning = 0;
    }
  };
  cron.schedule(cron_str, runner, utils.cron_conf);
};
const run_miss_cron = () => {
  let cron_str = "0 */10 * * * *";
  cyclic_depedency.print_cron_details(cron_str);
  const runner = async () => {
    if (lagrunning2 == 1) return console.log("race horses update running");
    try {
      lagrunning2 = 1;
      await update_dur(get_date_range_fromto(-1, "hour", -10, "minutes"));
      lagrunning2 = 0;
    } catch (err) {
      lagrunning2 = 0;
    }
  };
  cron.schedule(cron_str, runner, utils.cron_conf);
};

const test = async () => {
  console.log("racehorses test");
  // await push_ar([
  //   { hid: 425214, rid: "suMyYqZK", date: "2022-06-12T02:18:00", tc: 99 },
  //   { hid: 6651, rid: "suMyYqZK", date: "2022-06-12T02:18:00", tc: 99 },
  // ]);
  let ref = zed_db.db.collection(coll);
  ref.deleteMany({});
  console.log("done");
};

const main_runner = async () => {
  let [_node, _cfile, arg1, arg2, arg3, arg4, arg5, arg6, arg7] = process.argv;
  if (arg2 == "test") await test();
  if (arg2 == "lag") await update_lagging();
  if (arg2 == "dur")
    await update_dur(get_date_range_fromto(arg3, arg4, arg5, arg6));
  if (arg2 == "remain") await remain();
  if (arg2 == "run_cron") await run_cron();
  if (arg2 == "run_miss_cron") await run_miss_cron();
};

const race_horses = {
  push_ar,
  test,
  run_cron,
  main_runner,
};
module.exports = race_horses;
