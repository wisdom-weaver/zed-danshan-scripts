const _ = require("lodash");
const { zed_db, zed_ch } = require("../connection/mongo_connect");
const bulk = require("../utils/bulk");
const { get_races_n, get_races_of_hid } = require("../utils/cyclic_dependency");
const cyclic_depedency = require("../utils/cyclic_dependency");
const utils = require("../utils/utils");
const { dec } = require("../utils/utils");
const zedf = require("../utils/zedf");
const rating_blood = require("./rating_blood");

const name = "dp";
const coll = "dp4";
let cs = 25;
let test_mode = 0;

const wt_pos = {
  1: [6, 0.006],
  2: [5, 0.005],
  3: [4, 0.004],
  4: [3, 0.003],
  5: [2, 0.002],
  6: [1, 0.001],
  7: [-1, -0.001],
  8: [-2, -0.002],
  9: [-3, -0.003],
  10: [-4, -0.004],
  11: [-5, -0.005],
  12: [-6, -0.006],
  //pos: [choose, add]
};
const add_dist = {
  1000: 0.021,
  1200: 0.015,
  1400: 0.01,
  1600: 0,
  1800: 0.01,
  2000: 0.015,
  2200: 0.021,
  2400: 0.021,
  2600: 0.022,
};

const get_dist_pos_ob = async (hid, races = undefined) => {
  let ob = {};
  for (let d of [1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600]) {
    ob[d] = {};
    for (let p of [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]) {
      if (races == undefined) {
        ob[d][p] = await zed_ch.db.collection("zed").countDocuments({
          6: hid,
          1: { $in: [d, d.toString()] },
          8: { $in: [p, p.toString()] },
        });
      } else {
        let filt = _.filter(races, (i) => {
          return i.distance == d && i.place == p;
        });
        ob[d][p] = filt.length;
      }
    }
  }
  return ob;
};

const calc = async ({ hid, races = undefined }) => {
  try {
    hid = parseInt(hid);
    let races_n = await get_races_n(hid);
    if (test_mode) console.log({ races_n });
    if (!races_n) {
      let d = await zedf.race(hid);
      races_n = d.number_of_races;
      if (test_mode) console.log({ races_n });
      if (races_n !== 0) {
        await rating_blood.only([hid]);
      }
    }
    if (races_n === 0) return { hid, dp: null, dist: null };
    let dob = await get_dist_pos_ob(hid, races);
    if (test_mode) console.table(dob);
    let dist_rows = _.entries(dob).map(([dist, pos_ar]) => {
      // console.log(ar);
      let choose_dp = 0;
      let count = 0;
      for (let i = 1; i < 12; i++) {
        choose_dp += wt_pos[i][0] * pos_ar[i];
        count += pos_ar[i];
      }

      return { choose_dp, dist, count, pos_ar };
    });
    if (test_mode) console.table(dist_rows);
    let mx = _.maxBy(dist_rows, (e) => e.choose_dp);

    if (test_mode) console.log(mx);
    let { count, dist, pos_ar, choose_dp } = mx;
    dist = parseInt(dist);
    let add_dp =
      _.sum(
        _.entries(pos_ar).map(([p, c]) => {
          return c * wt_pos[p][1];
        })
      ) / count;
    let adder = add_dist[dist];
    let skill = count > 9 ? 10 * add_dp : count * add_dp;
    let dp = _.sum([skill, adder, add_dp]) * 65;
    if (test_mode)
      console.log({ dist, choose_dp, count, add_dp, adder, skill, dp });
    if (dp == 0 || dp == null || _.isNaN(dp)) {
      dp = null;
      dist = null;
    }
    let ob = { hid, dp, dist };
    return ob;
  } catch (err) {
    console.log(err.message);
    return null;
  }
};
const generate = async (hid) => {
  hid = parseInt(hid);
  // let races = await get_races_of_hid(hid);
  let ob = await calc({ hid, races: undefined });
  if (test_mode) console.log(ob);
  return ob;
};

const all = async () => bulk.run_bulk_all(name, generate, coll, cs, test_mode);
const only = async (hids) =>
  bulk.run_bulk_only(name, generate, coll, hids, cs, test_mode);
const range = async (st, ed) =>
  bulk.run_bulk_range(name, generate, coll, st, ed, cs, test_mode);

const fix = async () => {
  let hids = await cyclic_depedency.get_all_hids();
  hids = hids.slice(11400);
  await only(hids);
  // for (let chu of _.chunk(hids, 1000)) {
  //   let ar = await zed_db.db
  //     .collection(coll)
  //     .find({ hid: { $in: chu }, dp: 3 }, { projection: { hid: 1 } })
  //     .toArray();
  //   ar = _.map(ar, "hid");
  //   console.log(ar);
  //   if (!_.isEmpty(ar)) await only(ar);
  // }
  // console.log("Fixed");
};

const test = async (hids) => {
  test_mode = 1;
  for (let hid of hids) {
    let races = await get_races_of_hid(hid);
    // const ob = get_dist_pos_ob(hid, races);
    // let ob = await only([hid]);
    let ob = await calc({ hid, races });
    console.log(ob);
  }
};

const dp = { calc, generate, all, only, range, test, fix };
module.exports = dp;
