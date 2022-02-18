const _ = require("lodash");
const cron = require("node-cron");
const { zed_db } = require("../../connection/mongo_connect");
const { next_run, get_ed_horse } = require("../../utils/cyclic_dependency");
const cyclic_depedency = require("../../utils/cyclic_dependency");

const coll = "compiler_rng";
const name = "compiler_rng";
let t = 0;
const rng_rep = {
  "<.20": [-5, 0.2 - 1e-5],
  "0.20": [0.2, 0.4],
  "0.40": [0.4, 0.6],
  "0.60": [0.6, 0.8],
  "0.80": [0.8, 1.0],
  "1.00": [1.0, 1.2],
  "1.20": [1.2, 1.25],
  1.25: [1.25, 1.5],
  "1.50": [1.5, 2.0],
  "2.00": [2.0, 3.0],
  "3.00": [3.0, 4.0],
  "4.00": [4.0, 1e5],
};
let reps = _.keys(rng_rep);
const get_rng_rep = (n) => {
  if (!n) return null;
  for (let [rep, [mi, mx]] of _.entries(rng_rep)) {
    if (_.inRange(n, mi, mx + 1e-14)) return rep;
  }
};

const get_rng = async (hid) => {
  hid = parseInt(hid);
  let doc =
    (await zed_db.db
      .collection("gap4")
      .findOne({ hid }, { projection: { gap: 1 } })) ?? {};
  return doc?.gap ?? null;
};

const run_h = async (hid) => {
  try {
    hid = parseInt(hid);
    if (hid < 213000) return;

    let races_n = await cyclic_depedency.get_races_n(hid);
    if (races_n < 5) return;

    let parents = await cyclic_depedency.get_parents(hid);
    if (!parents) return;
    let { mother, father } = parents;

    let rng_m = await get_rng(mother);
    let rng_rep_m = get_rng_rep(rng_m);
    let rng_f = await get_rng(father);
    let rng_rep_f = get_rng_rep(rng_f);

    let comb_rng = null;
    if (rng_m !== null && rng_f !== null) comb_rng = rng_f + rng_m;
    let comb_rng_rep = null;
    if (rng_rep_m !== null && rng_rep_f !== null) {
      let a = rng_rep_f == "<.20" ? 0.2 : parseFloat(rng_rep_f);
      let b = rng_rep_m == "<.20" ? 0.2 : parseFloat(rng_rep_m);
      console.log(a, b);
      comb_rng_rep = a + b;
    }

    let bucket = `${rng_rep_f} - ${rng_rep_m}`;
    let doc = {
      bucket,
      rng_m,
      rng_f,
      comb_rng,
      comb_rng_rep,
      rng_rep_f,
      rng_rep_m,
    };
    if (t == 0)
      await zed_db.db
        .collection("gap4")
        .updateOne({ hid }, { $set: { compiler: doc } });
    console.log(name, hid, bucket, comb_rng_rep);
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
  console.log("compiler", [st, ed]);
  if (ed == null) ed = await get_ed_horse();

  let hids = new Array(ed - st + 1).fill(0).map((e, i) => st + i);
  // console.log(hids);
  await run_hs(hids);
};

const run = async () => {
  console.log("compiler run");
  for (let rng_rep_m of reps)
    for (let rng_rep_f of reps) {
      let docs =
        (await zed_db.db
          .collection("gap4")
          .find(
            {
              "compiler.rng_rep_m": { $eq: rng_rep_m, $exists: true },
              "compiler.rng_rep_f": { $eq: rng_rep_f, $exists: true },
            },
            {
              projection: { hid: 1, compiler: 1, gap: 1 },
            }
          )
          .toArray()) ?? [];

      let hids = _.map(docs, "hid");
      let tot = hids.length;
      let comb_rng_avg =
        _.mean(
          _.map(docs, "compiler.comb_rng").filter(
            (e) => ![null, undefined, NaN].includes(e)
          )
        ) ?? null;
      let baby_rng =
        _.mean(
          _.map(docs, "gap").filter((e) => ![null, undefined, NaN].includes(e))
        ) ?? null;

      let comb_rng_rep = rng_rep_f + rng_rep_m;
      let bucket = `${rng_rep_f} - ${rng_rep_m}`;
      let doc = {
        bucket,
        rng_rep_m,
        rng_rep_f,
        tot,
        hids,
        comb_rng_rep,
        comb_rng_avg,
        baby_rng
      };
      if (t == 0)
        await zed_db.db
          .collection(coll)
          .updateOne({ bucket }, { $set: doc }, { upsert: true });
      console.log(name, bucket, { tot, comb_rng_rep, comb_rng_avg });
    }
};

const run_cron = async () => {
  const cron_str = "*/5 * * * *";
  console.log("compiler next run ::", next_run(cron_str));
  cron.schedule(cron_str, run, { scheduled: true });
};

const test = async () => {
  t = 1;
  let hids = [213002];
  for (let hid of hids) {
    await run_h(hid);
  }
};

const compiler_rng = {
  run,
  run_cron,
  test,
  run_h,
  run_hs,
  run_range,
};
module.exports = compiler_rng;
