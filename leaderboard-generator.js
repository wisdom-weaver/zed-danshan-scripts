const _ = require("lodash");
const { MongoClient } = require("mongodb");
const mongoose = require("mongoose");
const prompt = require("prompt-sync")();
const fetch = require("node-fetch");

const {
  get_races_of_hid,
  calc_blood_hr,
} = require("./index-odds-generator");
const { run_func } = require("./index-run");
const { calc_avg } = require("./utils");
let zed_db = mongoose.connection;

const compare_horses_on_common_races_avgs = async ({ hid1, hid2, dist }) => {
  let race1 = await get_races_of_hid(hid1);
  race1 = _.keyBy(race1, "raceid");
  let keys1 = Object.keys(race1);
  let race2 = await get_races_of_hid(hid2);
  race2 = _.keyBy(race2, "raceid");
  let keys2 = Object.keys(race2);
  let comm_rids = _.intersection(keys1, keys2);
  comm_rids = comm_rids.filter((rid) => {
    if (race1[rid].distance !== race2[rid].distance) return false;
    if (dist != "All" && race1[rid].distance != dist) return false;
    return true;
  });
  let odds_table = comm_rids.map((rid) => ({
    odd1: race1[rid]?.odds,
    odd2: race2[rid]?.odds,
  }));
  const avg1 = calc_avg(odds_table.map((ea) => ea.odd1));
  const avg2 = calc_avg(odds_table.map((ea) => ea.odd2));
  return { avg1, avg2 };
};

const reorder_based_commom_races_avgs = async ({ ar, dist }) => {
  for (let i = 0; i < ar.length - 1; i++) {
    let a = ar[i];
    let b = ar[i + 1];

    const { hid: hid1 } = a;
    const { hid: hid2 } = b;
    const comp_ob = await compare_horses_on_common_races_avgs({
      hid1,
      hid2,
      dist,
    });
    const { avg1, avg2 } = comp_ob;
    let check = parseFloat(avg1) - parseFloat(avg2) > 0.5;
    let dec2 = (n) => parseFloat(n).toFixed(2);
    if (check) {
      ar[i] = b;
      ar[i + 1] = a;
    }
  }
  return ar;
};

const get_all_ratings_of_hid = async (hid) => {
  let { odds_live } =
    (await zed_db.db.collection("odds_live").findOne({ hid })) || {};
  if (_.isEmpty(odds_live) || _.isEmpty(_.compact(_.values(odds_live))))
    return null;
  let dists = [1000, 1200, 1400, 1600, 1800, 2000, 2400, 2600];
  let hrs = dists.map((d) => {
    let hr = calc_blood_hr({ odds_live, races_n: 1, override_dist: d });
    return hr;
  });
  // console.log(hrs);
  hrs = _.keyBy(hrs, "d");
  hrs["All"] = calc_blood_hr({ odds_live, races_n: 1 });
  hrs["All"] = { ...hrs["All"], type: "all" };
  // console.log(hrs);
  return hrs;
};

const get_horse_rating_ob_for_hids = async (hids) => {
  let all_ar = [];
  for (let hid of hids) {
    const horse_rating_ob = await get_all_ratings_of_hid(hid);
    if (_.isEmpty(horse_rating_ob));
    else {
      let ob = { hid, horse_rating_ob };
      console.log("got", hid);
      all_ar.push(ob);
    }
  }
  return all_ar;
};

const generate_all_dists_leaderboard = async () => {
  let dists = ["All", 1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600].map(
    (ea) => ea.toString()
  );

  // if (!dists.includes(dist)) return;
  let mx = 50000;
  const hids = new Array(mx + 1).fill(0).map((a, i) => i);
  // let hids = [0, 5, 3312, 15147];
  console.log(hids);
  // console.log({ hids });
  let all_ar = await get_horse_rating_ob_for_hids(hids);

  // console.log(all_ar);

  for (let dist of dists) {
    let ar = all_ar.map(({ hid, horse_rating_ob }) => {
      let hr = horse_rating_ob[dist];
      if (_.isEmpty(hr)) return null;
      return { hid, horse_rating: hr };
    });
    ar = _.compact(ar);
    ar = _.sortBy(ar, (item) => item.horse_rating?.cf || "na");
    ar = _.groupBy(ar, (item) => item.horse_rating?.cf || "na");
    let keys = Object.keys(ar).sort();
    let new_ar = [];
    for (let key of keys) {
      ar[key] = _.sortBy(ar[key], (o) => +o.horse_rating?.med || -1);
    }
    ar = _.flatten(_.values(ar));
    ar = ar.slice(0, 100);
    if (dist != "All") ar = await reorder_based_commom_races_avgs({ ar, dist });

    ar = ar.map((ea, idx) => ({ rank: idx + 1, ...ea }));
    let date = new Date();
    let id = `leaderboard-${dist}`;
    let ob = {
      id,
      date: date.getTime(),
      date_str: date.toISOString(),
      "leaderboard-dist": dist,
      leaderboard: ar,
    };
    await zed_db.db
      .collection("leaderboard")
      .updateOne({ id }, { $set: ob }, { upsert: true });
    console.log("dist", dist, "complete");
    // write_to_path_fast({
    //   file_path: leaderboard_out_path(dist),
    //   data: ob,
    // });
  }
  console.log("#finished all dists leaderboard generation");
  return;
};

const run_leaderboard = () => {
  run_func(generate_all_dists_leaderboard);
};

module.exports = {
  generate_all_dists_leaderboard,
  run_leaderboard,
};
