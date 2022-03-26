const _ = require("lodash");
const { zed_db, zed_ch } = require("../connection/mongo_connect");
const bulk = require("../utils/bulk");
const cyclic_depedency = require("../utils/cyclic_dependency");
const { get_races_n, get_races_of_hid } = require("../utils/cyclic_dependency");
const name = "rcount";
const coll = "rcount";
let cs = 25;
let test_mode = 0;

const dists = [1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600, 9000];
const cls = [0, 1, 2, 3, 4, 5, 6, 99, 77];
const poss = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 33];
const flas = [0, 1];
const get_dist_pos_ob = (hid, races = undefined) => {
  let ob = {};
  for (let d of dists) {
    ob[d] = {};
    let dist_races = [];
    dist_races =
      d == 9000 ? races : _.filter(races, (i) => i.distance == d) ?? [];
    for (let p of poss) {
      let filt =
        p == 33 ? dist_races : _.filter(dist_races, (i) => i.place == p);
      let free_r = _.filter(filt, (i) => parseFloat(i.entryfee) == 0);
      let paid_r = _.filter(filt, (i) => parseFloat(i.entryfee) !== 0);
      let free = free_r?.length || 0;
      let paid = paid_r?.length || 0;
      let flame = _.filter(filt, (i) => i.flame == 1)?.length || 0;
      let nflame = filt.length - flame;
      let free_flame = _.filter(free_r, (i) => i.flame == 1)?.length || 0;
      let free_nflame = free - free_flame;
      let paid_flame = _.filter(paid_r, (i) => i.flame == 1)?.length || 0;
      let paid_nflame = free - paid_flame;
      let n = filt.length;
      let ea = {
        0: n,
        1: free,
        2: paid,
        3: flame,
        4: nflame,
        5: free_flame,
        6: free_nflame,
        7: paid_flame,
        8: paid_nflame,
      };
      if (test_mode) ea = _.values(ea).join("-");
      ob[d][p] = ea;
    }
  }
  return ob;
};

const calc = async ({ hid, races = undefined }) => {
  try {
    // let dist_ob = get_dist_pos_ob(hid, races);
    let dist_ob = {};
    for (let c of cls) {
      let craces = c == 77 ? races : _.filter(races, (i) => i.thisclass == c);
      dist_ob[c] = get_dist_pos_ob(hid, craces);
      if (test_mode) {
        console.log(c == 77 ? "ALL" : `Class:${c}`);
        console.table(dist_ob[c]);
        console.log("-----");
      }
    }
    let ob = { hid, dist_ob };
    return ob;
  } catch (err) {
    console.log(err);
    console.log(err.message);
    return null;
  }
};
const generate = async (hid) => {
  hid = parseInt(hid);
  let races = await get_races_of_hid(hid);
  let ob = await calc({ hid, races });
  if (test_mode) console.log(ob);
  return ob;
};

const all = async () => bulk.run_bulk_all(name, generate, coll, cs, test_mode);
const only = async (hids) =>
  bulk.run_bulk_only(name, generate, coll, hids, cs, test_mode);
const range = async (st, ed) =>
  bulk.run_bulk_range(name, generate, coll, st, ed, cs, test_mode);

const fix = async () => {
  let stable = "0xa0d9665E163f498082Cd73048DA17e7d69Fd9224";
  let hids = await cyclic_depedency.get_owner_horses_zed_hids({ oid: stable });
  for (let hid of hids) {
    await only([hid]);
    console.log("done", hid);
  }
};

const test = async (hids) => {
  test_mode = 1;
  for (let hid of hids) {
    let races = await get_races_of_hid(hid);
    let ob = await calc({ hid, races });
  }
};

const rcount = { calc, generate, all, only, range, test, fix };
module.exports = rcount;
