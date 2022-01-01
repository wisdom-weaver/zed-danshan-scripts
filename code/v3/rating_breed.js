const _ = require("lodash");
const { init, zed_db, zed_ch } = require("../connection/mongo_connect");
const {
  calc_avg,
  pad,
  calc_median,
  fetch_r,
  struct_race_row_data,
  read_from_path,
  write_to_path,
  dec,
} = require("../utils/utils");
const app_root = require("app-root-path");
const { download_eth_prices, get_at_eth_price_on } = require("../utils/base");
const { options } = require("../utils/options");
const {
  get_races_of_hid,
  get_ed_horse,
} = require("../utils/cyclic_dependency");
const ymca2_s = require("./ymca2");
const global_req = require("../global_req/global_req");

let mx = 11000;
let st = 1;
let ed = mx;
let chunk_size = 25;
let chunk_delay = 100;

//global
let z_ALL;
let ymca2_avgs;

let tot_runs = 1;
const coll = "rating_breed3";
let test_mode = 0;

const get_reqs = () => {
  console.log("get_reqs");
  z_ALL = global_req.get_data().z_ALL;
  ymca2_avgs = global_req.get_data().ymca2_avgs;
};

const get_parents_hids = async (hid) => {
  hid = parseInt(hid);
  let { parents } = await zed_db.db
    .collection("horse_details")
    .findOne({ hid }, { projection: { _id: 0, parents: 1 } });
  return parents;
};

const geno = (genotype = "") => {
  if (genotype?.startsWith("Z")) genotype = genotype.slice(1);
  return parseInt(genotype) ?? null;
};

const get_kids_existing = async (hid) => {
  try {
    hid = parseInt(hid);
    let { offsprings = [] } =
      (await zed_db.db
        .collection("horse_details")
        .findOne({ hid }, { projection: { _id: 0, offsprings: 1 } })) || {};
    let ob = await Promise.all(
      offsprings.map((hid) =>
        zed_db.db.collection("horse_details").findOne(
          { hid },
          {
            projection: {
              _id: 0,
              hid: 1,
              genotype: 1,
              bloodline: 1,
              breed_type: 1,
              parents: 1,
            },
          }
        )
      )
    );
    ob = ob.map((e) => {
      if (_.isEmpty(e)) return null;
      let z = geno(e.genotype);
      return { ...e, z, ...e.parents };
    });
    ob = _.compact(ob);
    return ob;
  } catch (err) {
    console.log(err);
    return [];
  }
};

const get_breed_rating = async (hid) => {
  if (hid === null) return null;
  let { br = null } =
    (await zed_db.db
      .collection(coll)
      .findOne({ hid }, { projection: { _id: 0, br: 1 } })) || {};
  return br;
};

const get_g_odds = async (hid) => {
  hid = parseInt(hid);
  // let coll = hid <= 82000 ? "odds_overall" : "odds_overall2";
  let coll = "odds_overall2";
  let ob = await zed_db.db
    .collection(coll)
    .findOne({ hid }, { projection: { "odds_overall.0#####": 1, _id: 0 } });
  let { odds_overall = {} } = ob || {};
  let g = odds_overall["0#####"] || null;
  return g;
};

const get_z_med_ymca2 = async ({ bloodline, breed_type, genotype }) => {
  let id = "";
  let z = genotype.slice(1);
  z = "z" + pad(z, 3, 0).toString();
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

const get_ymca_global_avg = async ({ bloodline, breed_type, genotype }) => {
  let id = `${bloodline}-${breed_type}-${genotype}`;
  return ymca2_avgs[id]?.avg || null;
};

const calc = async ({ hid }) => {
  if (!z_ALL || !ymca2_avgs) get_reqs();
  try {
    let ymca2 = await ymca2_s.generate(hid);
    ymca2 = ymca2.ymca2;
    hid = parseInt(hid);
    if (hid == null || isNaN(hid)) return null;
    let kids = (await get_kids_existing(hid)) || [];
    // console.log({ hid, kids });
    if (_.isEmpty(kids)) {
      let empty_kg = {
        hid,
        odds: {},
        avg: null,
        br: null,
        kids_n: 0,
        is: null,
        ymca2,
      };
      if (test_mode)
        console.log(
          "# hid:",
          hid,
          "kids_n:",
          0,
          "br:",
          null,
          "ymca2:",
          dec(ymca2, 2)
        );
      return empty_kg;
    }

    let kids_n = _.isEmpty(kids) ? 0 : kids.length;
    let kids_hids = _.map(kids, "hid");

    let kids_scores_ob = await Promise.all(
      kids.map((kid) => ymca2_s.generate(kid.hid))
    );
    kids_scores_ob = _.chain(kids_scores_ob)
      .keyBy("hid")
      .mapValues("ymca2")
      .value();
    if (test_mode) console.log("kids_scores_ob", kids_scores_ob);

    let gavg_ob = await Promise.all(
      kids.map((kid) =>
        get_ymca_global_avg(kid).then((gavg) => [kid.hid, gavg])
      )
    );
    gavg_ob = Object.fromEntries(gavg_ob);
    if (test_mode) console.log("gavg_ob", gavg_ob);

    let op_br_ob = await Promise.all(
      kids.map((kid) => {
        let { father, mother } = kid.parents;
        let op = hid == father ? mother : father;
        return get_breed_rating(op).then((op_br) => [kid.hid, op_br]);
      })
    );
    op_br_ob = Object.fromEntries(op_br_ob);
    if (test_mode) console.log("op_br_ob", op_br_ob);

    kids = kids.map((e) => ({
      ...e,
      ymca2: kids_scores_ob[e.hid],
      gavg: gavg_ob[e.hid],
      op_br: op_br_ob[e.hid],
    }));
    kids = kids.map((e) => {
      let fact;
      if (e.ymca2 == 0 || e.gavg == 0 || _.isNaN(e.ymca2) || _.isNaN(e.gavg))
        fact = null;
      else fact = e.ymca2 / e.gavg;
      let adj;
      if (fact == null) adj = null;

      if (e.op_br == null || _.isNaN(e.op_br)) {
        adj = fact;
      } else {
        adj = e.op_br > 1.1 ? fact * 0.9 : e.op_br < 0.9 ? fact * 1.1 : fact;
      }
      let good_adj;
      if (adj == 0) good_adj = 0;
      else good_adj = e.ymca2 > e.gavg ? 0.1 : -0.1;
      return { ...e, fact, adj, good_adj };
    });
    if (test_mode) console.table(kids);

    let avg = _.chain(kids_scores_ob).values().compact().mean().value();
    let br = _.chain(kids).map("adj").values().compact().value();
    if (br.length == 0) br = null;
    else br = _.mean(br);

    br += _.chain(kids).map("good_adj").compact().sum().value();

    let kg = {
      hid,
      odds: kids_scores_ob,
      avg,
      br,
      kids_n,
      ymca2,
    };
    // console.log({ avg: dec2(avg), br: dec2(br) });
    if (test_mode)
      console.log(
        "# hid:",
        hid,
        "kids_n:",
        kids_n,
        "br:",
        dec(br, 2),
        "ymca2:",
        dec(ymca2, 2)
      );
    return kg;
  } catch (err) {
    console.log("err on horse get_kg", hid);
    console.log(err);
    return null;
  }
};
const generate = async (hid) => {
  let ob = await calc({ hid });
  return ob;
};
const test = async (hids) => {
  test_mode = 1
  for (let hid of hids) {
    let ob = await generate(hid);
    console.log(hid, ob);
  }
};
const rating_breed = {
  generate,
  calc,
  test,
};
module.exports = rating_breed;
