const _ = require("lodash");
const cron = require("node-cron");
const { zed_db } = require("../connection/mongo_connect");
const { next_run } = require("../utils/cyclic_dependency");
const { dp } = require("../v3/v3");

const coll = "compiler";
const name = "compiler";

const run_h = async (hid) => {
  try {
    hid = parseInt(hid);
    if (hid < 213000) return;
    let hdoc = await zed_db.db
      .collection("horse_details")
      .findOne({ hid }, { projection: { parents: 1 } });
    // console.log(hid, hdoc);
    let { parents = null } = hdoc;
    if (_.isEmpty(parents)) return;
    let { mother, father } = parents;
    if (mother == null || father == null) return;

    await dp.only([hid]);
    let dp_m = await zed_db.db.collection("dp4").findOne({ hid: mother });
    if (_.isEmpty(dp_m)) {
      await dp.only([mother]);
      dp_m = await zed_db.db.collection("dp4").findOne({ hid: mother });
    }
    let dp_f = await zed_db.db.collection("dp4").findOne({ hid: father });
    if (_.isEmpty(dp_f)) {
      await dp.only([father]);
      dp_f = await zed_db.db.collection("dp4").findOne({ hid: father });
    }
    // console.log(dp_f)
    // console.log(dp_m)
    let dist_m = dp_m.dist;
    let dist_f = dp_f.dist;
    let bucket = `${dist_f / 100}-${dist_m / 100}`;
    await zed_db.db.collection("dp4").updateOne(
      { hid },
      {
        $set: {
          compiler: { dist_m, dist_f, bucket },
        },
      }
    );
    console.log(hid, { dist_m, dist_f, bucket });
  } catch (err) {
    console.log(hid, err);
  }
};
const run_hs = async (hids) => {
  for (let chu of _.chunk(hids, 45)) {
    await Promise.all(chu.map(run_h));
  }
};
const run_range = async ([st, ed]) => {
  let hids = Array.from(ed - st + 1)
    .fill(0)
    .map((e, i) => st + i);
  await run_hs(hids);
};

const run = async () => {
  console.log("compiler run");
  let dists = [null, 1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600];
  for (let dist_f of dists)
    for (let dist_m of dists) {
      let docs =
        (await zed_db.db
          .collection("dp4")
          .find(
            {
              "compiler.dist_f": { $eq: dist_f, $exists: true },
              "compiler.dist_m": { $eq: dist_m, $exists: true },
            },
            {
              projection: { hid: 1, compiler: 1, dist: 1 },
            }
          )
          .toArray()) ?? [];
      let dist_seg = _.groupBy(docs, "dist");
      let ob = {};
      for (let [d, ar] of _.entries(dist_seg)) {
        let n = ar.length;
        let hids = _.map(ar, "hid");
        let per = (n ?? 0) / (docs.length || 1);
        ob[d] = { n, hids, per };
      }
      let bucket = `${dist_f / 100}-${dist_m / 100}`;
      let tot = docs.length;
      let doc = {
        bucket,
        tot,
        dist_ob: ob,
      };
      await zed_db.db
        .collection("compiler")
        .updateOne({ bucket }, { $set: doc }, { upsert: true });
      console.log(bucket, { tot });
    }
};

const run_cron = async () => {
  const cron_str = "0 */5 * * *";
  console.log("compiler next run ::", next_run(cron_str));
  cron.schedule(cron_str, run, { scheduled: true });
};

const test = async () => {};

const compiler = {
  run,
  run_cron,
  test,
  run_h,
  run_hs,
  run_range,
};
module.exports = compiler;
