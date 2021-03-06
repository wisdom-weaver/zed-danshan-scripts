const _ = require("lodash");
const cron = require("node-cron");
const { zed_db } = require("../../connection/mongo_connect");
const { next_run, get_ed_horse, print_cron_details } = require("../../utils/cyclic_dependency");
const { dp } = require("../../v3/v3");
const compiler_common = require("./compiler_common");
const v_code = compiler_common.v_code;
let t = compiler_common.t;

const coll = "compiler_dp";
const name = "compiler_dp";

const run_h = async (hid) => {
  try {
    hid = parseInt(hid);
    let hdoc = await zed_db.db
      .collection("horse_details")
      .findOne({ hid }, { projection: { parents: 1, tx_date: 1 } });
    // console.log(hid, hdoc);
    if (_.isEmpty(hdoc)) return;
    if (hdoc?.tx_date < compiler_common.st_date) return;

    let { parents = null } = hdoc;
    if (_.isEmpty(parents)) return;
    let { mother, father } = parents;
    if (mother == null || father == null) return;

    // await dp.only([hid]);
    let dp_m = await zed_db.db.collection("dp4").findOne({ hid: mother });
    let dist_m = dp_m?.dist || null;
    // if (_.isEmpty(dp_m) || dist_m == null) {
    //   await dp.only([mother]);
    //   dp_m = await zed_db.db.collection("dp4").findOne({ hid: mother });
    //   dist_m = dp_m?.dist || null;
    // }

    let dp_f = await zed_db.db.collection("dp4").findOne({ hid: father });
    let dist_f = dp_f?.dist || null;
    // if (_.isEmpty(dp_f) || dist_f == null) {
    //   await dp.only([father]);
    //   dp_f = await zed_db.db.collection("dp4").findOne({ hid: father });
    //   dist_f = dp_f?.dist || null;
    // }

    // console.log(dp_f)
    // console.log(dp_m)
    let bucket = `${dist_f / 100}-${dist_m / 100}`;
    const doc = { dist_m, dist_f, bucket, v_code };
    if (t == 0)
      await zed_db.db.collection("dp4").updateOne(
        { hid },
        {
          $set: {
            compiler: doc,
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
const run_horses = async () => {
  const hids = await compiler_common.get_compiler_hids();
  console.log("compiler hids:", hids.length);
  // console.log(hids);
  await run_hs(hids);
};

const run = async () => {
  console.log("compiler run");
  let dists = [1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600];
  for (let dist_f of dists)
    for (let dist_m of dists) {
      let docs =
        (await zed_db.db
          .collection("dp4")
          .find(
            {
              "compiler.v_code": { $eq: v_code, $exists: true },
              "compiler.dist_f": { $eq: dist_f, $exists: true },
              "compiler.dist_m": { $eq: dist_m, $exists: true },
              // dist: { $ne: { $in: [null, NaN] } },
            },
            {
              projection: { hid: 1, compiler: 1, dist: 1 },
            }
          )
          .toArray()) ?? [];
      docs = _.filter(docs, (i) => i.dist !== null);
      let dist_seg = _.groupBy(docs, "dist");
      // if (dist_seg && dist_seg["null"]) delete dist_seg["null"];

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
        dist_f,
        dist_m,
        tot,
        dist_ob: ob,
      };
      if (t == 0)
        await zed_db.db
          .collection(coll)
          .updateOne({ bucket }, { $set: doc }, { upsert: true });
      console.log(name, bucket, { tot });
    }
};

const runner = async () => {
  await run_horses();
  await run();
};
const run_cron = async () => {
  const cron_str = "*/30 * * * *";
  print_cron_details(cron_str)
  cron.schedule(cron_str, runner, { scheduled: true });
};

const test = async () => {
  let bucket = "16-18";
  let doc = await zed_db.db.collection(coll).findOne({ bucket });
  console.log({ bucket });
  console.table(doc.dist_ob);
};

const compiler_dp = {
  run,
  runner,
  run_cron,
  test,
  run_h,
  run_hs,
  run_horses,
};
module.exports = compiler_dp;
