const { get_races_of_hid } = require("../utils/cyclic_dependency");
const mdb = require("../connection/mongo_connect");
const _ = require("lodash");
const bulk = require("../utils/bulk");
const { zed_db } = require("../connection/mongo_connect");
const coll = "rating_flames3";
const name = "rating_flames v3";
const cs = 200;
let test_mode = 0;

let keys = (() => {
  let classes = [1, 2, 3, 4, 5];
  let dists = [1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600];
  let fee_tags = ["A", "B", "C", "D", "E"];
  let keys = [];
  for (let c of classes)
    for (let f of fee_tags) for (let d of dists) keys.push(`${c}${f}${d}`);
  return keys;
})();

const get_points_from_place = (p) => {
  p = parseInt(p);
  switch (p) {
    case 1:
      return 4;
    case 2:
      return 3;
    case 3:
      return 2;
    case 4:
      return 0;
    case 5:
      return -1;
    case 6:
      return -2;
    case 7:
      return -3;
    case 8:
      return -4;
    case 9:
      return 0;
    case 10:
      return 1;
    case 11:
      return 2;
    case 12:
      return 3;
    default:
      0;
  }
};
const get_side = (ob) => {
  let side;
  let tc = ob?.tc;
  let rc = parseInt(ob?.key[0]);
  if (!rc) return null;
  if (rc == tc) side = "C";
  else if (rc < tc) side = "B";
  else if (rc > tc) side = "A";
  if (side == "C" && tc == 1) side = "B";
  return side;
};
const def_rating_flames = {
  key: null,
  cf: null,
  // races_n: null,
  // cfd_n: null,
  // flames_n: null,
  flames_per: null,
  avg_points: null,
  side: null,
};
const calc = async ({ hid, races = [], tc }) => {
  try {
    hid = parseInt(hid);
    let races_n = races.length || 0;
    if (races?.length == 0)
      return { hid, tc, ...def_rating_flames, races_n, rated_type: "NR" };
    // console.table(races);
    let ob = {};
    for (let key of keys) {
      let c = parseInt(key[0]);
      let f = key[1];
      let F = 1;
      let d = parseInt(key.slice(2));
      let fr = races.filter((r) => {
        if (r.thisclass !== c) return false;
        if (r.fee_tag !== f) return false;
        if (r.distance !== d) return false;
        return true;
      });
      let fr_F = fr.filter((r) => r.flame == F);
      if (fr_F.length <= 2) continue;

      let points_ar = fr_F.map((e) => get_points_from_place(e.place));
      let tot_points = _.sum(points_ar);
      let avg_points = _.mean(points_ar);
      // console.log(key, avg_points)
      ob[key] = {
        cf: `${c}${f}`,
        races_n,
        cfd_n: fr.length,
        flames_n: fr_F.length,
        tot_points,
        avg_points,
      };
    }
    ob = _.entries(ob).map(([key, e]) => ({
      key,
      ...e,
    }));
    ob = ob.map((ea) => {
      let flames_per = (ea.flames_n * 100) / ea.cfd_n;
      flames_per = parseFloat(flames_per).toFixed(2);
      flames_per = parseFloat(flames_per);
      let { key, cf, avg_points } = ea;
      return { key, cf, avg_points, flames_per };
    });
    ob = _.orderBy(
      ob,
      ["cf", "flames_per", "avg_points"],
      ["asc", "desc", "desc"]
    );

    if (_.isEmpty(ob))
      return { ...def_rating_flames, hid, tc, races_n, rated_type: "CH" };
    // console.table(ob);
    let mx_ob = ob[0];
    // console.table(mx_ob);
    let fob = { hid, ...mx_ob, tc, races_n, rated_type: "GH" };
    fob.side = get_side(fob);
    return fob;
  } catch (err) {
    console.log("err on get_rating_flames", hid);
    console.log(err);
  }
};
const generate = async (hid) => {
  hid = parseInt(hid);
  let races = await get_races_of_hid(hid);
  let doc =
    (await zed_db.db.collection("horse_details").findOne({ hid }, { tc: 1 })) ||
    null;
  if (doc == null) return null;
  let tc = doc?.tc;
  let ob = await calc({ races, tc, hid });
  // console.log(ob);
  return ob;
};
const all = async () => bulk.run_bulk_all(name, generate, coll, cs, test_mode);
const only = async (hids) =>
  bulk.run_bulk_only(name, generate, coll, hids, cs, test_mode);
const range = async (st, ed) =>
  bulk.run_bulk_range(name, generate, coll, st, ed, cs, test_mode);

const test = async (hids) => {
  for (let hid of hids) {
    test_mode = 1;
    let ob = await generate(hid);
    console.log(hid, ob);
  }
};

const rating_flames = {
  test,
  calc,
  generate,
  all,
  only,
  range,
};
module.exports = rating_flames;
