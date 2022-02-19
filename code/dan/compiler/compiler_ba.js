const _ = require("lodash");
const cron = require("node-cron");
const { zed_db } = require("../../connection/mongo_connect");
const { next_run, get_ed_horse } = require("../../utils/cyclic_dependency");
const cyclic_depedency = require("../../utils/cyclic_dependency");

const coll = "compiler_ba";
const name = "compiler_ba";
const st = 213000;
let t = 0;

const ba_rep = {
  "<1.00": [-1e4, 1.0 - +1e-5],
  "01.50": [1.0, 1.5],
  "02.00": [1.5, 2.0],
  "02.50": [2.0, 2.5],
  "03.00": [2.5, 3.0],
  "03.50": [3.0, 3.5],
  "04.00": [3.5, 4.0],
  ">4.00": [4.0 + 1e-5, 1e4],
};
let reps = _.keys(ba_rep);
const get_ba_rep = (n) => {
  if (!n) return null;
  for (let [rep, [mi, mx]] of _.entries(ba_rep)) {
    if (_.inRange(n, mi, mx + 1e-14)) return rep;
  }
};

const get_ba = async (hid) => {
  hid = parseInt(hid);
  let doc =
    (await zed_db.db
      .collection("rating_blood3")
      .findOne({ hid }, { projection: { "base_ability.n": 1 } })) ?? {};
  return doc?.base_ability?.n ?? null;
};
const conv_num = (n) => {
  return parseFloat(n.includes(">") || n.includes("<") ? n.slice(1) : n);
};

const run_h = async (hid) => {
  try {
    hid = parseInt(hid);
    if (hid < st) return;

    let races_n = await cyclic_depedency.get_races_n(hid);
    if (races_n < 5) return;

    let parents = await cyclic_depedency.get_parents(hid);
    if (!parents) return;
    let { mother, father } = parents;

    let ba_m = await get_ba(mother);
    let ba_rep_m = get_ba_rep(ba_m);
    let ba_f = await get_ba(father);
    let ba_rep_f = get_ba_rep(ba_f);

    let comb_ba = null;
    if (ba_m !== null && ba_f !== null) comb_ba = ba_f + ba_m;
    let comb_ba_rep = null;
    if (ba_rep_m !== null && ba_rep_f !== null) {
      let a = conv_num(ba_rep_m);
      let b = conv_num(ba_rep_f);
      comb_ba_rep = a + b;
    }

    let bucket = `${ba_rep_f} - ${ba_rep_m}`;
    let doc = {
      bucket,
      ba_m,
      ba_f,
      comb_ba,
      comb_ba_rep,
      ba_rep_f,
      ba_rep_m,
    };
    if (t == 0)
      await zed_db.db
        .collection("rating_blood3")
        .updateOne({ hid }, { $set: { compiler: doc } });
    console.log(name, hid, bucket, comb_ba_rep);
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
  for (let ba_rep_m of reps)
    for (let ba_rep_f of reps) {
      let docs =
        (await zed_db.db
          .collection("rating_blood3")
          .find(
            {
              "compiler.ba_rep_m": { $eq: ba_rep_m, $exists: true },
              "compiler.ba_rep_f": { $eq: ba_rep_f, $exists: true },
            },
            {
              projection: { hid: 1, compiler: 1, "base_ability.n": 1 },
            }
          )
          .toArray()) ?? [];

      let hids = _.map(docs, "hid");
      let tot = hids.length;
      let comb_ba_avg =
        _.mean(
          _.map(docs, "compiler.comb_ba").filter(
            (e) => ![null, undefined, NaN].includes(e)
          )
        ) ?? null;
      if (_.isNaN(comb_ba_avg)) comb_ba_avg = null;
      let baby_ba_avg =
        _.mean(
          _.map(docs, "base_ability.n").filter(
            (e) => ![null, undefined, NaN].includes(e)
          )
        ) ?? null;
      if (_.isNaN(baby_ba_avg)) baby_ba_avg = null;

      let comb_ba_rep = null;
      if (ba_rep_m !== null && ba_rep_f !== null) {
        let a = conv_num(ba_rep_f);
        let b = conv_num(ba_rep_m);
        comb_ba_rep = a + b;
      }

      let bucket = `${ba_rep_f} - ${ba_rep_m}`;
      let doc = {
        bucket,
        ba_rep_m,
        ba_rep_f,
        tot,
        hids,
        comb_ba_rep,
        comb_ba_avg,
        baby_ba_avg,
      };
      if (t == 0)
        await zed_db.db
          .collection(coll)
          .updateOne({ bucket }, { $set: doc }, { upsert: true });
      console.log(name, bucket, { tot, comb_ba_rep, comb_ba_avg });
    }
};

const runner = async () => {
  await run_range([st]);
  await run();
};
const run_cron = async () => {
  const cron_str = "0 * * * *";
  console.log("compiler next run ::", next_run(cron_str));
  cron.schedule(cron_str, runner, { scheduled: true });
};

const test = async () => {
  t = 1;
  let hids = [213002];
  for (let hid of hids) {
    await run_h(hid);
  }
};

const compiler_ba = {
  run,
  runner,
  run_cron,
  test,
  run_h,
  run_hs,
  run_range,
};
module.exports = compiler_ba;
