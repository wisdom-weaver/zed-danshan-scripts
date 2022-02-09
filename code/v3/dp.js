const _ = require("lodash");
const { zed_db, zed_ch } = require("../connection/mongo_connect");
const bulk = require("../utils/bulk");
const { dec } = require("../utils/utils");

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

const get_dist_pos_ob = async (hid) => {
  let ob = {};
  for (let d of [1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600]) {
    ob[d] = {};
    for (let p of [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]) {
      ob[d][p] = await zed_ch.db.collection("zed").countDocuments({
        6: hid,
        1: { $in: [d, d.toString()] },
        8: { $in: [p, p.toString()] },
      });
    }
  }
  return ob;
};

const calc = async ({ hid }) => {
  try {
    let dob = await get_dist_pos_ob(hid);
    // console.log(dob)
    let dist_rows = _.entries(dob).map(([d, ar]) => {
      // console.log(ar);
      let pts = _.entries(ar).map(([p, n]) => parseInt(n) * wt_p[p]);
      pts = _.sum(pts);
      return { pts, dist: d };
    });
    // console.log(dist_rows);
    let mx = _.maxBy(dist_rows, (e) => e.pts);
    // console.log(mx);
    let { pts, dist } = mx;
    let dp = pts * wt_d[dist] * 0.0001;
    dist = parseInt(dist);
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

const test = async (hids) => {
  test_mode = 1;
  for (let hid of hids) {
    let ob = await only([hid]);
    console.log(ob);
  }
};

const dp = { calc, generate, all, only, range, test };
module.exports = dp;
