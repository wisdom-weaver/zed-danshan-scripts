const _ = require("lodash");
const { zed_db, zed_ch } = require("../connection/mongo_connect");
const bulk = require("../utils/bulk");
const { get_races_n, get_races_of_hid } = require("../utils/cyclic_dependency");
const cyclic_depedency = require("../utils/cyclic_dependency");
const utils = require("../utils/utils");
const { dec } = require("../utils/utils");
const { race } = require("../utils/zedf");

const name = "dp";
const coll = "dp4";
let cs = 25;
let test_mode = 0;

const wt_p = {
  1: 12,
  2: 11,
  3: 10,
  4: 9,
  5: 8,
  6: 7,
  7: 6,
  8: 5,
  9: 4,
  10: 3,
  11: 2,
  12: 1,
};
const wt_d = {
  1000: 3,
  1200: 2,
  1400: 1,
  1600: 0,
  1800: 1,
  2000: 2,
  2200: 3,
  2400: 3.05,
  2600: 3.1,
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
    let races_n = await get_races_n(hid);
    if (races_n === 0) return { hid, dp: null, dist: null, pts: 0 };
    let dob = await get_dist_pos_ob(hid, races);
    if (test_mode) console.table(dob);
    let dist_rows = _.entries(dob).map(([d, ar]) => {
      // console.log(ar);
      let diff = [];
      for (let i = 1; i < 12; i++) {
        diff[i] = ar[i] - ar[i + 1];
      }
      let pts = _.mean(diff);
      return { pts, dist: d };
    });
    if (test_mode) console.log(dist_rows);
    let mx = _.maxBy(dist_rows, (e) => e.pts);
    if (test_mode) console.log(mx);
    let { pts, dist } = mx;
    let dp = pts + wt_d[dist];
    dist = parseInt(dist);
    if (pts == 0 || pts == null || _.isNaN(pts)) {
      dp = null;
      dist = null;
    }
    let ob = { hid, dp, dist, pts };
    return ob;
  } catch (err) {
    console.log(err.message);
    return null;
  }
};
const generate = async (hid) => {
  hid = parseInt(hid);
  let ob = await calc({ hid });
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
  for (let chu of _.chunk(hids, 1000)) {
    let ar = await zed_db.db
      .collection(coll)
      .find({ hid: { $in: chu }, dp: 3 }, { projection: { hid: 1 } })
      .toArray();
    ar = _.map(ar, "hid");
    console.log(ar);
    if (!_.isEmpty(ar)) await only(ar);
  }
  console.log("Fixed");
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
