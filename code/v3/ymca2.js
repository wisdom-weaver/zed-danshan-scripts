const _ = require("lodash");
const { zed_db, zed_ch } = require("../connection/mongo_connect");
const global_req = require("../global_req/global_req");
const bulk = require("../utils/bulk");
const { struct_race_row_data } = require("../utils/cyclic_dependency");
const { calc_race_score } = require("./race_score");

let test_mode = 0;
let cs = 100;
let z_ALL;
let ymca2_avgs;
const name = "ymca2";
const coll = "rating_breed3";
const first_n_races = 8;

const get_reqs = () => {
  console.log("get_reqs");
  z_ALL = global_req.get_data().z_ALL;
  ymca2_avgs = global_req.get_data().ymca2_avgs;
};

const get_z_med = async ({ bloodline, breed_type, genotype }) => {
  if (!genotype) return null;
  let id = "";
  let z = genotype?.slice(1);
  z = "z" + z.toString().padStart(3, "0");
  bloodline = bloodline.toString().toLowerCase();
  breed_type = breed_type.toString().toLowerCase();
  id = `${z}-${bloodline}-${breed_type}`;
  if (_.isEmpty(z_ALL)) {
    let z_med_doc =
      (await zed_db.db.collection("z_meds").findOne({ id })) || {};
    let { med: z_med = 0 } = z_med_doc;
    return z_med;
  } else {
    let z_med = z_ALL[id];
    return z_med;
  }
};

const get_z_ALL_meds = async () => {
  let doc = await zed_db.db.collection("z_meds").findOne({ id: "z_ALL" });
  let ob = _.chain(doc.ar).keyBy("id").mapValues("med").value();
  return ob;
};

const calc = async ({ hid, races = [], details }) => {
  try {
    if (!z_ALL || !ymca2_avgs) get_reqs();
    if (_.isEmpty(races)) return null;
    races = _.sortBy(races, "date");
    races = races.slice(0, first_n_races);
    let avg_z = await get_z_med(details);
    if (test_mode) console.log("avg_z", avg_z);
    let r_ob = races.map((r) => {
      let { thisclass: rc, fee_tag, place: position, flame } = r;
      let score = calc_race_score({ rc, fee_tag, position, flame });
      let final_score = score * 0.1 - avg_z * 0.02;
      return {
        rc,
        fee_tag,
        position,
        flame,
        score,
        final_score,
      };
    });
    if (test_mode) console.table(r_ob);

    let ymca2 = _.meanBy(r_ob, "final_score") ?? null;
    if (test_mode) console.log(hid, "ymca2", ymca2);

    return ymca2;
  } catch (err) {
    console.log("err in ymca2", hid, err);
    return null;
  }
};

const generate = async (hid) => {
  try {
    hid = parseInt(hid);
    let details = await zed_db.db
      .collection("horse_details")
      .findOne({ hid }, { bloodline: 1, breed_type: 1, genotype: 1 });
    if (!details) {
      console.log(hid, "details missing");
      return null;
    }
    if (test_mode) console.log(details);

    let races = await zed_ch.db
      .collection("zed")
      .find({ 6: hid })
      .sort({ 2: 1 })
      .limit(first_n_races)
      .toArray();
    races = struct_race_row_data(races);
    if (_.isEmpty(races)) return { hid, ymca2: null };

    if (test_mode) console.table(races);

    const ymca2 = await calc({ hid, races, details });
    let ob = { hid, ymca2 };
    return ob;
  } catch (err) {
    console.log("err in get_kids_score", err);
  }
};

const all = async () => {
  console.log(name, "all");
  bulk.run_bulk_all(name, generate, coll, cs, test_mode);
};
const only = async (hids) => {
  console.log(name, "only");
  bulk.run_bulk_only(name, generate, coll, hids, cs, test_mode);
};
const range = async (st, ed) => {
  console.log(name, "range");
  bulk.run_bulk_range(name, generate, coll, st, ed, cs, test_mode);
};

const test = async (hids) => {
  console.log(name, "test");
  test_mode = 1;
  for (let hid of hids) {
    let ob = await generate(hid);
    console.log(hid, ob);
  }
};

const ymca2_s = { calc, generate, test, get_z_med, all, only, range };
module.exports = ymca2_s;
