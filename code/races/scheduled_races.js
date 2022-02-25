const moment = require("moment");
const cron = require("node-cron");
const zedf = require("../utils/zedf");
const _ = require("lodash");
const utils = require("../utils/utils");
const { zed_db } = require("../connection/mongo_connect");
const races_base = require("./races_base");
const cyclic_depedency = require("../utils/cyclic_dependency");

const coll = "sraces";

const scheduled_api = (c, offset = 0) =>
  `https://racing-api.zed.run/api/v1/races?status=scheduled&offset=${offset}&class=${c}`;
const get_sraces = (c, offset) => zedf.get(scheduled_api(c, offset));
const get_scheduled_races_c = async (c) => {
  let races = [];
  let offset = 0;
  do {
    let n_races = (await get_sraces(c, offset)) || [];
    offset += n_races.length;
    if (_.isEmpty(n_races)) return races;
    races = [...races, ...n_races];
  } while (true);
  return races;
};
const get_all = async () => {
  // let all = [];
  // for (let c of [0, 1, 2, 3, 4, 5, 6, 99]) {
  //   let curr = await get_scheduled_races_c(c);
  //   console.log(`C${c}:`, curr.length);
  //   all = [...all, ...curr];
  // }
  let ar = await Promise.all(
    [0, 1, 2, 3, 4, 5, 6, 99].map(get_scheduled_races_c)
  );
  let all = _.flatten(ar);
  console.log(`scheduled:`, all.length);
  return all;
};

const push = async (sraces) => {
  if (_.isEmpty(sraces)) return;
  let ar = sraces.map((e) => {
    let rid = e.race_id;
    let start_time = e.start_time;
    if (!start_time.endsWith("Z")) start_time += "Z";
    let hids = _.map(e.gates);
    return { rid, hids, start_time };
  });
  let bulk = ar.map((e) => {
    return {
      updateOne: {
        filter: { rid: e.rid },
        update: { $set: e },
        upsert: true,
      },
    };
  });
  if (!_.isEmpty(bulk)) {
    let resp = await zed_db.db.collection(coll).bulkWrite(bulk);
    console.log("wrote %d to %s", bulk.length, coll);
  } else {
    console.log("no races to write");
  }
};

const process = async () => {
  let st = moment().subtract("3", "minutes").toISOString();
  console.log("processing scheduled before", st);
  let rids =
    (await zed_db.db
      .collection(coll)
      .find(
        { start_time: { $lte: st }, processing: 0 },
        { projection: { rid: 1 } }
      )
      .toArray()) ?? [];
  rids = _.map(rids, "rid");
  await zed_db.db
    .collection(coll)
    .updateMany({ rid: { $in: rids } }, { $set: { processing: 1 } });
  console.log("races to-process:", rids.length);
  let evals = await races_base.zed_race_rids(rids);
  console.log("races processed:", evals.length);
  await zed_db.db.collection(coll).deleteMany({ rid: { $in: evals } });
};

const runner = async () => {
  let sraces = await get_all();
  await push(sraces);
  await process();
};

const test = async () => {
  // runner();
  run_cron();
};

const run_cron = async () => {
  let cron_str = "0 * * * * *";
  cyclic_depedency.print_cron_details(cron_str);
  cron.schedule(cron_str, runner, { scheduled: true });
};

const scheduled_races = {
  test,
  get_all,
  runner,
  test,
  run_cron,
};

module.exports = scheduled_races;
