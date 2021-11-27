const _ = require("lodash");
const fetch = require("node-fetch");
const app_root = require("app-root-path");
const { get_races_of_hid } = require("./cyclic_dependency");
const { zed_db, init } = require("./index-run");
const { dec, nano } = require("./utils");

let rat_bl_seq = {
  cls: [1, 2, 3, 4, 5],
  fee: ["A", "B", "C", "D", "E"],
  dists: [1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600],
  tunnels: ["S", "M", "D"],
};

let days = 45;
let days_ed = 60;

const generate_rating_blood_calc = async ({ hid, races = [] }, p) => {
  hid = parseInt(hid);
  let now = Date.now();
  let rat_nano = now - days * 24 * 60 * 60 * 1000;
  let rat_nano_ed = now - days_ed * 24 * 60 * 60 * 1000;

  let races_ed = _.filter(races, (i) => {
    return _.inRange(nano(i.date), rat_nano_ed, rat_nano);
  });
  let races_r = _.filter(races, (i) => {
    return _.inRange(nano(i.date), rat_nano, now);
  });
  console.log("races_r", races_r.length, races_r[0]?.date);
  console.log("races_ed", races_ed.length, races_ed[0]?.date);

  races = races_r;

  if (_.isEmpty(races_ed) && _.isEmpty(races)) {
    let nr_ob = { cf: null, d: null, med: null, rated_type: "NR" };
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
        console.log(cf, d, n, p1);
        if (p1 == 0) continue;
        let p2 =
          _.filter(fr, (i) => ["2"].includes(i.place.toString()))?.length || 0;
        let p12_ratio = p1 / (p2 || 1);
        let win_rate = (p1 / (n || 1)) * 100;
        let flame_rate = (flames / (n || 1)) * 100;
        ar.push({ cf, d, races_n: n, p12_ratio, win_rate, flames, flame_rate });
        // console.log(str);
      }
      // console.table(ar);
    }
  }
  console.table(ar);
  if (!_.isEmpty(ar)) {
    ar = _.sortBy(ar, [
      (i) => i.cf,
      (i) => -i.d,
      (i) => -i.p12_ratio,
      (i) => -i.win_rate,
      (i) => -i.flame_rate,
    ]);
    let ob = ar[0];
    return { ...ob, rated_type: "GH" };
  }
  let ch_ob = { cf: null, d: null, med: null, rated_type: "CH" };
  return ch_ob;
};

const generate_rating_blood = async ({ hid, races, tc }, p) => {
  let ob = await generate_rating_blood_calc({ hid, races }, p);
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
  console.log({ hid, tc, len: races.length });
  console.table(races);
  let ob = await generate_rating_blood({ hid, races, tc });
  ob.name = name;
  console.log("#hid:", hid, ob.rated_type, get_blood_str(ob));
  return ob;
};
const get_blood_str = (ob) => {
  try {
    let { cf, d, side, p12_ratio, win_rate, flame_rate, rated_type } = ob;
    if (rated_type == "GH")
      return `${cf}-${d}-${dec(p12_ratio)}-${dec(win_rate)}-${dec(flame_rate)}`;
    return rated_type;
  } catch (err) {
    return "err";
  }
};

const runner = async () => {
  await init();
  let hid = 132134;
  let ob = await generate_rating_blood_from_hid(hid);
  console.log(ob);
};
runner();

module.exports = {
  generate_rating_blood,
  generate_rating_blood_from_hid,
  generate_rating_blood_calc,
};
