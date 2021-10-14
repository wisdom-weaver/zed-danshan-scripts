const _ = require("lodash");
const fetch = require("node-fetch");
const mongoose = require("mongoose");
const { init, zed_db, zed_ch, run_func } = require("./index-run");
const {
  write_to_path,
  read_from_path,
  struct_race_row_data,
  side_text,
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
};

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

const get_rat_score = ({ c, f, d, races, cf_fee }, p) => {
  let races_n = races.length;

  let tag_price = fee_tags_ob[f][0];

  // 1st WIn % *1
  // 2nd Win % *.6
  // 3rd win % *.45
  // 11th Win % *.2
  // 12th WIn % .3
  let p_nums = _.chain(place_fact)
    .entries()
    .map(([p, fact]) => {
      p = p.toString();
      let per = _.filter(races, (i) => i.place?.toString() == p);
      per = ((per?.length || 0) * 100) / races_n;
      per *= fact;
      if (!per || _.isNaN(per)) per = 0;
      return [`#${p}`, per];
    })
    .fromPairs()
    .value();

  // Entry Fee X4 (max 200)
  let feeX4 = cf_fee * 4;
  if (feeX4 > 200) feeX4 = 200;

  // Number of Races X 1 (max10)
  let races_Nx = races_n;
  if (races_Nx > 10) races_Nx = 10;

  if (p)
    console.table([
      {
        key: `${c}${f}${d}`,
        races_n,
        tag_price,
        ...p_nums,
        cf_fee,
        feeX4,
        races_Nx,
      },
    ]);

  let perf = 0;
  perf = _.sum([..._.values(p_nums), feeX4, races_Nx]) / 100;
  return perf;
};

const generate_rating_blood_calc = async ({ hid, races = [] }, p) => {
  hid = parseInt(hid);
  races = races.map((e) => {
    let entryfee_usd = parseFloat(e.entryfee) * get_at_eth_price_on(e.date);
    let fee_tag = get_fee_tag(entryfee_usd);
    return { ...e, entryfee_usd, fee_tag };
  });

  if (_.isEmpty(races)) {
    let nr_ob = { cf: null, d: null, med: null, rated_type: "NR" };
    return nr_ob;
  }

  // console.log(races[0]);
  for (let c of rat_bl_seq.cls) {
    for (let f of rat_bl_seq.fee) {
      let ar = [];
      let cf_fee = _.chain(races)
        .filter({ thisclass: c, fee_tag: f })
        .map((i) => parseFloat(i?.entryfee_usd))
        .compact()
        .mean()
        .value();
      for (let d of rat_bl_seq.dists) {
        let fr = _.filter(races, {
          thisclass: c,
          distance: d,
          fee_tag: f,
          flame: 1,
        });
        let fee_tag_price = fee_tags_ob[f][0];
        let key = `C${c}-D${d.toString().slice(0, 2)}-$${fee_tag_price}${f}`;

        if (fr.length >= 3) {
          rat_score = get_rat_score({ c, f, d, races: fr, cf_fee }, p);
        } else rat_score = null;

        if (rat_score !== null) {
          if (p) console.log({ key, rat_score });
          ar.push({ key, rat_score, c, f, d, len: fr.length });
        }
      }
      if (!_.isEmpty(ar)) {
        let mx = _.maxBy(ar, "rat_score");
        let { c, f, d, rat_score } = mx;
        let ob = { cf: `${c}${f}`, d, med: rat_score, rated_type: "GH" };
        return ob;
      }
    }
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
  let races = await zed_ch.db.collection("zed").find({ 6: hid }).toArray();
  races = struct_race_row_data(races);
  // console.log({ hid, tc, len: races.length });
  let ob = await generate_rating_blood({ hid, races, tc });
  ob.name = name;
  console.log("#hid:", hid, ob.rated_type, get_blood_str(ob));
  return ob;
};
const get_blood_str = (ob) => {
  try {
    let { cf, d, med, side, tc, rated_type } = ob;
    if (rated_type == "GH")
      return `C${cf[0]}-D${d.toString().slice(0, 2)}-${dec(med, 2)}`;
    return rated_type;
  } catch (err) {
    return "err";
  }
};

const runner = async () => {
  await initiate();
  // await odds_generator_all_horses();
  // await breed_generator_all_horses();
  // clone_odds_overall();
  let hids = [21888];
  // await odds_generator_for_hids(hids);
  for (let hid of hids) {
    let ob = await generate_rating_blood_from_hid(hid);
    console.log(hid, ob);
  }
};
// runner();

module.exports = {
  generate_rating_blood,
  generate_rating_blood_from_hid,
};
