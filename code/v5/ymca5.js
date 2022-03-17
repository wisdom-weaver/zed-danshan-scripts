const _ = require("lodash");
const { zed_db, zed_ch } = require("../connection/mongo_connect");
const global_req = require("../global_req/global_req");
const bulk = require("../utils/bulk");
const cyclic_depedency = require("../utils/cyclic_dependency");
const {
  struct_race_row_data,
  get_races_n,
} = require("../utils/cyclic_dependency");
const utils = require("../utils/utils");
const dp_scr = require("../v3/dp");
const { calc_race_score, calc_race_score_det } = require("../v3/race_score");

let test_mode = 0;
let cs = 200;
let ymca2_avgs;
const name = "ymca5";
const coll = "ymca5";
const fraces_n = 30;
const fraces_n_dp = 8;

const get_reqs = () => {
  console.log("get_reqs");
  ymca2_avgs = global_req.get_data().ymca2_avgs;
};

const get_z_avg_of_race = async (rid) => {
  let docs = await zed_ch.db
    .collection("zed")
    .find({ 4: rid }, { projection: { 6: 1 } })
    .toArray();
  let hids = _.map(docs, 6);
  let docs2 = await zed_db.db
    .collection("horse_details")
    .find(
      { hid: { $in: hids } },
      { projection: { genotype: 1, hid: 1, _id: 0 } }
    )
    .toArray();
  let avg_z = _.map(docs2, "genotype");
  avg_z = avg_z.map((e) => utils.geno(e));
  avg_z = _.mean(avg_z);
  return avg_z;
};

const calc_inner = async ({ hid, races }) => {
  if (_.isEmpty(races)) return null;
  let rids = _.map(races, "raceid");
  let avg_z_ob = await Promise.all(
    rids.map((rid) => get_z_avg_of_race(rid).then((d) => [rid, d]))
  );
  avg_z_ob = _.fromPairs(avg_z_ob);
  let r_ob = races.map((r) => {
    let { thisclass: rc, fee_tag, place: position, flame, raceid } = r;
    let score = calc_race_score_det({ rc, fee_tag, position, flame });
    let avg_z = Math.min(avg_z_ob[raceid], 29);
    let sc = score?.sc;
    let final_score = sc * 0.1 - avg_z * 0.02;
    return {
      rc,
      fee_tag,
      position,
      flame,
      avg_z,
      ...score,
      final_score,
    };
  });
  if (test_mode) console.table(r_ob);

  let ymca5 = _.meanBy(r_ob, "final_score") ?? null;
  if (ymca5 !== null) ymca5 = Math.max(ymca5, 0.05);
  if (test_mode) console.log(hid, "ymca5", ymca5);
  return ymca5;
};

const calc = async ({ hid, races = [], from = null }) => {
  try {
    if (_.isEmpty(races)) return null;
    let filt_races;
    if (!ymca2_avgs) get_reqs();

    if (from == "mega") {
      if (races.length >= fraces_n) {
        let ddist = null;
        let i = 2;
        do {
          let dp4 = await zed_db.db
            .collection("dp4")
            .findOne({ hid }, { projection: { dist: 1 } });
          ddist = dp4?.dist;
          // if (!ddist) await dp_scr.only([hid]);
        } while (i-- && ddist !== null);
        if (!ddist) {
          filt_races = races.slice(0, fraces_n);
        } else {
          let draces = _.filter(races, (i) => i.distance == ddist);
          draces = draces.slice(0, fraces_n_dp);
          filt_races = draces;
        }
      } else {
        filt_races = races.slice(0, fraces_n);
      }
    } else if (from == "generate") filt_races = races;
    else return undefined;

    races = _.sortBy(races, "date");
    races = races.slice(0, fraces_n);
    const ymca5 = await calc_inner({ hid, races: filt_races });
    return ymca5;
  } catch (err) {
    console.log("err in ymca5", hid, err);
    return null;
  }
};

const generate = async (hid) => {
  try {
    hid = parseInt(hid);
    let details = await zed_db.db
      .collection("horse_details")
      .findOne(
        { hid },
        { projection: { bloodline: 1, breed_type: 1, genotype: 1 } }
      );
    if (!details) {
      console.log(hid, "details missing");
      return null;
    }
    if (test_mode) console.log(details);

    const blooddoc = await zed_db.db
      .collection("rating_blood3")
      .findOne({ hid }, { projection: { races_n: 1 } });
    const races_n = blooddoc?.races_n || 0;

    let races = [];
    if (races_n >= fraces_n) {
      if (test_mode) console.log("dp mode");
      let ddist = null;
      let i = 2;
      do {
        let dp4 = await zed_db.db
          .collection("dp4")
          .findOne({ hid }, { projection: { dist: 1 } });
        ddist = dp4?.dist;
        // if (!ddist) await dp_scr.only([hid]);
      } while (i-- && ddist !== null);
      if (test_mode) console.log({ ddist });
      if (!ddist)
        races = await zed_ch.db
          .collection("zed")
          .find({ 6: hid })
          .sort({ 2: 1 })
          .limit(fraces_n)
          .toArray();
      else
        races = await zed_ch.db
          .collection("zed")
          .find({ 6: hid, 1: { $in: [parseInt(ddist), ddist?.toString()] } })
          .sort({ 2: 1 })
          .limit(fraces_n_dp)
          .toArray();
    } else {
      if (test_mode) console.log("still new mode");
      races = await zed_ch.db
        .collection("zed")
        .find({ 6: hid })
        .sort({ 2: 1 })
        .limit(fraces_n)
        .toArray();
    }
    races = struct_race_row_data(races);
    if (_.isEmpty(races)) return { hid, ymca5: null };

    if (test_mode) console.table(races);

    const ymca5 = await calc({ hid, races, details, from: "generate" });
    let ob = { hid, ymca5 };
    return ob;
  } catch (err) {
    console.log("err in ymca5", err);
  }
};

const all = async () => {
  console.log(name, "all");
  await bulk.run_bulk_all(name, generate, coll, cs, test_mode);
};
const only = async (hids) => {
  console.log(name, "only");
  await bulk.run_bulk_only(name, generate, coll, hids, cs, test_mode);
};
const range = async ([st, ed]) => {
  console.log(name, "range", st, ed);
  await bulk.run_bulk_range(name, generate, coll, st, ed, cs, test_mode);
};

const test = async (hids) => {
  console.log(name, "test");
  test_mode = 1;
  for (let hid of hids) {
    let ob = await generate(hid);
    console.log(hid, ob);
  }
};

const fix = async (hid) => {
  // let races_n = await get_races_n(hid);
  // let doc = await zed_db.db
  //   .collection(coll)
  //   .findOne({ hid }, { projection: { ymca5: 1 } });
  // let ymca5 = doc?.ymca5 ?? null;
  // console.log({ races_n, ymca5 });
  // if (races_n > 0 && ymca5 == null) {
  //   console.log("fixing", hid);
  //   await only([hid]);
  // }
};
const fixer = async () => {
  let all_hids = await cyclic_depedency.get_all_hids();
  // let all_hids = [46092];
  for (let chunk of _.chunk(all_hids, 1000)) {
    let [a, b] = [chunk[0], chunk[chunk.length - 1]];
    console.log(a, "->", b);
    await Promise.all(chunk.map(fix));
  }
  console.log("ENDED fixer");
};

const ymca5_s = { calc, generate, test, all, only, range, fixer };
module.exports = ymca5_s;
