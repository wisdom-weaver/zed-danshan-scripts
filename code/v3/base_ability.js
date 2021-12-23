const { get_races_of_hid } = require("../utils/cyclic_dependency");
const _ = require("lodash");
const bulk = require("../utils/bulk");
const { zed_db } = require("../connection/mongo_connect");
const { options } = require("../utils/options");
const { geno, dec } = require("../utils/utils");
const coll = "rating_blood3";
const name = "base_ability v3";
const cs = 200;
const test_mode = 0;

const c_tab = {
  0: 1,
  1: 7,
  2: 4.5,
  3: 3,
  4: 2,
  5: 1.5,
};
const f_tab = {
  A: 10,
  B: 10,
  C: 8,
  D: 5,
  E: 2.5,
  F: 1,
};

let pos_tab = {
  1: [6, 12],
  2: [5, 11],
  3: [4, 10],
  4: [3, 9],
  5: [2, 8],
  6: [1, 7],
  7: [0, 6],
  8: [0, 5],
  9: [0, 4],
  10: [0, 3],
  11: [0, 2],
  12: [0, 1],
};

const calc_base_ea_score = ({ rc, fee_tag, position, flame }) => {
  let c_sc = c_tab[rc] || 0;
  let f_sc = f_tab[fee_tag] || 0;
  let p_sc = pos_tab[position][flame] || 0;
  return c_sc + f_sc + p_sc;
};

const calc = async ({ hid, races = [], tc }) => {
  try {
    hid = parseInt(hid);
    let ob = { hid };
    let base_ability = 0;
    let filt_races =
      _.filter(races, (i) => {
        let { distance, thisclass } = i;
        if (thisclass == 99) return false;
        return ["1400", "1600", "1800"].includes(distance.toString());
      }) || [];
    // if (test_mode) console.log(filt_races.length);
    let r_ob = filt_races.map((r) => {
      let { thisclass: rc, fee_tag, place: position, flame } = r;
      let score = calc_base_ea_score({ rc, fee_tag, position, flame });
      let final_score = score * 0.1;
      return { final_score };
    });
    base_ability = _.meanBy(r_ob, "final_score") ?? null;
    if (_.isNaN(base_ability)) base_ability = null;
    ob.base_ability = base_ability;
    let conf = (filt_races?.length || 0) * 5;
    conf = Math.min(99, conf);
    ob.base_conf = conf;
    return ob;
  } catch (err) {
    console.log("err on rating", hid);
    console.log(err);
  }
};
const generate = async (hid) => {
  hid = parseInt(hid);
  let races = await get_races_of_hid(hid);
  // console.log(races[0]);
  let doc = await zed_db.db
    .collection("horse_details")
    .findOne({ hid }, { tc: 1 });
  let tc = doc?.tc || undefined;
  let ob = await calc({ races, tc, hid });
  if (test_mode) console.log(hid, ob);
  return ob;
};

const get_table_row = async (id) => {
  let [bl, bt, z] = id.split("-");
  let ar = await zed_db.db
    .collection("horse_details")
    .find(
      {
        bloodline: bl,
        breed_type: bt,
        genotype: z,
      },
      { projection: { _id: 0, hid: 1 } }
    )
    .toArray();
  let hids = _.map(ar, "hid") || [];
  let docs = await zed_db.db
    .collection("rating_blood3")
    .find({ hid: { $in: hids } }, { projection: { _id: 0, base_ability: 1 } })
    .toArray();

  let scores = _.chain(docs).map("base_ability").compact().value();

  let ba_avg = _.mean(scores);
  if (!ba_avg || _.isNaN(ba_avg)) ba_avg = null;
  let ba_min = _.min(scores);
  if (!ba_min || _.isNaN(ba_min)) ba_min = null;
  let ba_max = _.max(scores);
  if (!ba_max || _.isNaN(ba_max)) ba_max = null;

  let str = [ba_min, ba_avg, ba_max].map((e) => dec(e)).join(" ");

  console.log({ id, count: scores.length }, str);
  return {
    count_all: ar.length,
    count: scores.length,
    ba_min,
    ba_avg,
    ba_max,
  };
};
const generate_table = async () => {
  let ob = {};
  let keys = [];
  for (let bl of options.bloodline)
    for (let bt of options.breed_type)
      for (let z of options.genotype) {
        if (bt == "genesis" && geno(z) > 10) continue;
        if (bl == "Nakamoto" && bt == "legendary" && geno(z) > 4) continue;
        if (bl == "Szabo" && bt == "legendary" && geno(z) > 8) continue;
        if (bl == "Finney" && bt == "legendary" && geno(z) > 14) continue;
        if (bl == "Buterin" && bt == "legendary" && geno(z) > 20) continue;
        let id = `${bl}-${bt}-${z}`;
        keys.push(id);
      }
  // keys = ["Buterin-cross-Z13"];
  // keys = keys.slice(0, 5);
  for (let id of keys) {
    ob[id] = await get_table_row(id);
  }
  // return;
  let doc_id = "base-ability-global-table";
  await zed_db.db
    .collection("requirements")
    .updateOne(
      { id: doc_id },
      { $set: { id: doc_id, avg_ob: ob } },
      { upsert: true }
    );
  console.log("done");
};

const all = async () => bulk.run_bulk_all(name, generate, coll, cs, test_mode);
const only = async (hids) =>
  bulk.run_bulk_only(name, generate, coll, hids, cs, test_mode);
const range = async (st, ed) =>
  bulk.run_bulk_range(name, generate, coll, st, ed, cs, test_mode);

const test = async (hid) => {
  hid = parseInt(hid);
  // let hid = 126065;
  let races = await get_races_of_hid(hid);
  let ob = await calc_overall_rat({ hid, races });
  console.table(ob);
  let ob3 = await calc_tunnel_rat({ hid, races });
  console.table(ob3);
};

const base_ability = {
  test,
  calc,
  generate,
  all,
  only,
  range,
  generate_table,
};
module.exports = base_ability;
