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
const get_races_scheduled_c = async (c) => {
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
  //   let curr = await get_races_scheduled_c(c);
  //   console.log(`C${c}:`, curr.length);
  //   all = [...all, ...curr];
  // }
  let ar = await Promise.all(
    [0, 1, 2, 3, 4, 5, 6, 99].map(get_races_scheduled_c)
  );
  let all = _.flatten(ar);
  console.log(`scheduled:`, all.length);
  return all;
};

const push = async (sraces) => {
  if (_.isEmpty(sraces)) return;
  let ar = sraces.map((e) => {
    let rid = e.race_id;
    let length = e.length;
    let fee = parseFloat(e.fee);
    let start_time = e.start_time;
    let race_class = e.class;
    if (!start_time.endsWith("Z")) start_time += "Z";
    let hids = _.map(e.gates);
    let ob = { rid, hids, start_time, length, fee, race_class };
    // console.log(ob);
    return ob;
  });
  let bulk = ar.map((e) => {
    return {
      updateOne: {
        filter: { rid: e.rid },
        update: { $set: { ...e, processing: 0 } },
        upsert: true,
      },
    };
  });
  if (!_.isEmpty(bulk)) {
    let resp = await zed_db.db.collection(coll).bulkWrite(bulk);
    console.log(
      "wrote new:%d & upsert:%d to %s",
      resp?.insertedCount,
      resp?.upsertedCount,
      coll
    );
  } else {
    console.log("no races to write");
  }
};

const process = async () => {
  try {
    let st1 = moment().subtract("3", "minutes").toISOString();
    let st2 = moment().subtract("4", "minutes").toISOString();
    console.log("scheduled not_processed:%s  also:%s", st1, st2);
    let rids =
      (await zed_db.db
        .collection(coll)
        .find(
          {
            $or: [
              { start_time: { $lte: st1 }, processing: 0 },
              { start_time: { $lte: st2 } },
            ],
          },
          { projection: { rid: 1, _id: 1 } }
        )
        .toArray()) ?? [];

    rids = _.map(rids, "rid");

    let exists = await races_base.check_exists_rids(rids);
    console.log("exists:", exists.length);
    let missing = _.difference(rids, exists);
    console.log("missing:", missing.length);

    await zed_db.db.collection(coll).deleteMany({ rid: { $in: exists } });

    rids = missing;
    console.log("races to-process:", rids.length);
    if (rids.length == 0) return;

    await zed_db.db
      .collection(coll)
      .updateMany({ rid: { $in: rids } }, { $set: { processing: 1 } });
    let evals = await races_base.zed_race_run_rids(rids);
    console.log("races processed:", evals.length);
    await zed_db.db.collection(coll).deleteMany({ rid: { $in: evals } });
  } catch (err) {
    console.log("err process races_scheduled", err);
  }
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

const races_scheduled = {
  test,
  get_all,
  runner,
  process,
  test,
  run_cron,
};

module.exports = races_scheduled;
