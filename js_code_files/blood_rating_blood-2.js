const _ = require("lodash");
const fetch = require("node-fetch");
const mongoose = require("mongoose");
const { init, zed_db, zed_ch, run_func } = require("./index-run");
const {
  write_to_path,
  read_from_path,
  struct_race_row_data,
  side_text,
  nano,
} = require("./utils");
const app_root = require("app-root-path");
const {
  get_fee_cat_on,
  download_eth_prices,
  get_at_eth_price_on,
  get_date,
} = require("./base");
const { generate_max_horse } = require("./max_horses");
const {
  get_n_upload_rating_flames,
  generate_rating_flames,
} = require("./update_flame_concentration");
const { dec } = require("./utils");
const { generate_breed_rating, init_btbtz } = require("./horses-kids-blood2");
const { get_races_of_hid } = require("./cyclic_dependency");

let mx;
let st = 1;
let ed = mx;
let chunk_size = 25;
let chunk_delay = 100;

const initiate = async () => {
  await init();
  await download_eth_prices();
};

let fee_tags_ob = {
  A: [25.0, 17.5, 5000],
  B: [15.0, 12.5, 17.5],
  C: [10.0, 7.5, 12.5],
  D: [5.0, 3.75, 7.5],
  E: [2.5, 1.25, 3.75],
  F: [0.0, 0.0, 0.0],
};

let rat_bl_seq = {
  cls: [1, 2, 3, 4, 5],
  fee: ["A", "B", "C", "D", "E"],
  dists: [1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600],
  tunnels: ["S", "M", "D"],
};

let days = 60;
let days_ed = 60;

const get_fee_tag = (entryfee_usd) => {
  for (let [tag, [rep, mi, mx]] of _.entries(fee_tags_ob))
    if (_.inRange(entryfee_usd, mi, mx + 1e-3)) return tag;
};

const place_fact = {
  1: 1,
  2: 0.6,
  3: 0.45,
  11: 0.2,
  12: 0.3,
};

const generate_rating_blood_calc = async (
  { hid, races = [], for_leader = 0 },
  p
) => {
  hid = parseInt(hid);
  if (for_leader) {
    let now = Date.now();
    let rat_nano = now - days * 24 * 60 * 60 * 1000;
    let rat_nano_ed = now - days_ed * 24 * 60 * 60 * 1000;

    // let races_ed = _.filter(races, (i) => {
    //   return _.inRange(nano(i.date), rat_nano_ed, rat_nano);
    // });
    let races_r = _.filter(races, (i) => {
      return _.inRange(nano(i.date), rat_nano, now);
    });
    // console.log("races_r", races_r.length, races_r[0]?.date);
    // console.log("races_ed", races_ed.length, races_ed[0]?.date);

    races = races_r;
  }

  if (_.isEmpty(races)) {
    let nr_ob = {
      cf: null,
      d: null,
      p12_ratio: null,
      rat: null,
      win_rate: null,
      flame_rate: null,
      rated_type: "NR",
    };
    return nr_ob;
  }

  // console.log(races[0]);
  let ar = [];
  for (let c of rat_bl_seq.cls) {
    for (let f of rat_bl_seq.fee) {
      for (let d of rat_bl_seq.tunnels) {
        let fr = _.filter(races, {
          thisclass: c,
          tunnel: d,
          fee_tag: f,
          // flame: 1,
        });
        let n = fr.length;
        // if (n < 3) continue;
        let cf = `${c}${f}`;
        let flames = _.filter(fr, { flame: 1 })?.length || 0;
        let p1 =
          _.filter(fr, (i) => ["1"].includes(i.place.toString()))?.length || 0;
        // console.log(cf, d, n, p1);
        if (n < 5) continue;
        if (!p1 && p1 == 0) continue;
        let p = [
          "1",
          "2",
          "3",
          "4",
          "5",
          "6",
          "7",
          "8",
          "9",
          "10",
          "11",
          "12",
        ].map((e) => {
          let count =
            _.filter(fr, (i) => [e].includes(i.place.toString()))?.length || 0;
          return [e, count];
        });
        p = _.fromPairs(p);
        // console.log(cf, n, p1);
        let den =
          p[2] * 0.5 +
          p[3] * 0.625 +
          p[4] +
          p[5] * 1.1 +
          p[6] * 1.125 +
          p[7] * 1.25 +
          p[8] * 1.37 +
          p[9] * 0.9 +
          p[10] * 0.625 +
          p[11] * 0.5 +
          p[12] * 0.4;
        let p12_ratio;
        if (den == 0) p12_ratio = 0.5 * p1;
        else p12_ratio = p[1] / den + n * 0.001;

        let win_rate = (p1 / (n || 1)) * 100;
        let flame_rate = (flames / (n || 1)) * 100;
        let rat = ((flame_rate / 100) * 0.5 + p12_ratio) * 10;
        let ob = { cf, d, p12_ratio, win_rate, p1, flame_rate, rat, n };
        ar.push(ob);
        // console.log(get_blood_str(ob));
        // if (p1 >= 1) return { ...ob, rated_type: "GH" };
      }
      // console.table(ar);
      if (!_.isEmpty(ar)) {
        ar = _.orderBy(ar, [
          (i) => i.cf,
          (i) => -i.rat,
          (i) => -i.win_rate,
          (i) => -i.flame_rate,
        ]);
        let ob = ar[0];
        return { ...ob, rated_type: "GH" };
      }
    }
  }
  let ch_ob = {
    cf: null,
    d: null,
    rat: null,
    p12_ratio: null,
    win_rate: null,
    flame_rate: null,
    rated_type: "CH",
  };
  return ch_ob;
};

const generate_rating_blood = async ({ hid, races, tc }, p) => {
  let ob = await generate_rating_blood_calc({ hid, races, for_leader: 0 }, p);
  ob.hid = hid;
  ob.tc = tc;
  let side;
  if (ob.rated_type === "NR") side = "-";
  else if (ob.rated_type === "CH") side = "A";
  else if (ob.rated_type === "GH") {
    let rc = parseInt(ob.cf[0]);
    if (rc == tc) side = "C";
    else if (rc < tc) side = "B";
    else if (rc > tc) side = "A";
    if (side == "C" && tc == 1) side = "B";
    ob.side = side;
  }
  // console.log(hid, ob.rated_type, side_text(ob.side), get_blood_str(ob));
  return ob;
};
const generate_rating_blood_from_hid = async (hid) => {
  hid = parseInt(hid);
  let doc = await zed_db.db
    .collection("horse_details")
    .findOne({ hid }, { projection: { _id: 0, tc: 1, name: 1 } });
  if (_.isEmpty(doc)) {
    console.log("emp horse", hid);
    return null;
  }
  let { tc, name } = doc;
  let races = await get_races_of_hid(hid);
  // console.log({ hid, tc, len: races.length });
  let ob = await generate_rating_blood({ hid, races, tc });
  ob.name = name;
  console.log("#hid:", hid, ob.rated_type, get_blood_str(ob));
  return ob;
};
const get_blood_str = (ob) => {
  try {
    let { cf, d, side, p12_ratio, rat, win_rate, flame_rate, rated_type } = ob;
    if (rated_type == "GH")
      return `${cf}-${d}-${dec(rat)}-${dec(win_rate)}-${dec(flame_rate)}`;
    return rated_type;
  } catch (err) {
    return "err";
  }
};

const generate_rating_blood_dist_for_hid = async (hid) => {
  hid = parseInt(hid);
  let races = await get_races_of_hid(hid);
  let ob = await generate_rating_blood_dist({ hid, races });
  return ob;
};

const generate_rating_blood_dist = async ({ hid, races }) => {
  hid = parseInt(hid);
  let doc = await zed_db.db
    .collection("horse_details")
    .findOne({ hid }, { projection: { _id: 0, tc: 1, name: 1 } });
  if (_.isEmpty(doc)) {
    console.log("emp horse", hid);
    return null;
  }
  let { tc, name } = doc;
  let ob = {};
  ob.hid = hid;
  ob.name = name;
  for (let dist of ["All", ...rat_bl_seq.tunnels]) {
    let fr = dist == "All" ? races : _.filter(races, { tunnel: dist });
    ob[dist] = await generate_rating_blood_calc({
      hid,
      races: fr,
      for_leader: 1,
    });
  }
  console.log("done hid:", hid);
  return ob;
};

const runner = async () => {
  await init();
  let hid = 34750;
  let ob = await generate_rating_blood_from_hid(hid);
  console.log(ob);
  let ob2 = await generate_rating_blood_dist_for_hid(hid);
  console.log(ob2);
};
// runner();

module.exports = {
  generate_rating_blood,
  generate_rating_blood_from_hid,
  generate_rating_blood_calc,
  generate_rating_blood_dist_for_hid,
  generate_rating_blood_dist,
  get_fee_tag,
  get_blood_str,
};
