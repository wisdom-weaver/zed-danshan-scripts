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
const bulk = require("../utils/bulk");
const cyclic_depedency = require("../utils/cyclic_dependency");
const utils = require("../utils/utils");
const ymca2_table = require("./ymca2_table");

let mx = 11000;
let st = 1;
let ed = mx;
let cs = 25;
let chunk_delay = 100;

//global
let z_ALL;
let ymca2_avgs;

let tot_runs = 1;
const name = "rating_breed";
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
let logi_bonus_ar = [
  [0.1, 0.5, 0.6],
  [0.2, 0.6, 0.7],
  [0.3, 0.7, 0.8],
  [0.4, 0.8, 0.9],
  [0.5, 0.9, 0.99],
  [0.52, 1.0, 1.0],
];

const get_logi_bonus = (logi) => {
  for (let [val, mi, mx] of logi_bonus_ar)
    if (_.inRange(logi, mi, mx + 1e-14)) return val;
  return 0;
};

const N = 10;
const log_fact = 0.07;
const bonus_cap = 0.9;
const bad_fact = 0.8;

const calc = async ({ hid }) => {
  if (!z_ALL || !ymca2_avgs) get_reqs();
  try {
    // let ymca2 = await ymca2_s.generate(hid);
    // ymca2 = ymca2.ymca2;
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
        // ymca2,
      };
      if (test_mode)
        console.log(
          "# hid:",
          hid,
          "kids_n:",
          0,
          "br:",
          null
          // "ymca2:",
          // dec(ymca2, 2)
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
      if (e.ymca2 == 0 || _.isNaN(e.ymca2)) fact = null;
      else if (e.gavg == 0 || _.isNaN(e.gavg)) fact = e.ymca2;
      else fact = e.ymca2 / e.gavg;
      let adj;
      if (fact == null) adj = null;

      if (e.op_br == 0 || _.isNaN(e.op_br)) adj = fact;
      else if (e.op_br > 1.05) adj = fact * 0.98;
      else if (e.op_br >= 1 && e.op_br < 1.05) adj = fact * 0.99;
      else if (e.op_br > 0.9 && e.op_br < 0.9999999) adj = fact * 1;
      else if (e.op_br > 0.80000001 && e.op_br < 0.89999999) adj = fact * 1.01;
      else if (e.op_br < 0.8) adj = fact * 1.02;
      else adj = null;

      let good_adj;
      let diff = (e.ymca2 || 0) - (e.gavg || 0);
      if (!adj) good_adj = 0;
      else if (diff < -0.1) good_adj = -0.1;
      else if (diff > 0.1) good_adj = 0.1;
      else good_adj = diff;

      return { ...e, fact, adj, good_adj };
    });
    if (test_mode) console.table(kids);

    let avg = _.chain(kids_scores_ob).values().compact().mean().value();
    let adjs = _.chain(kids).map("adj").values().compact().value();
    let good_adjs = _.chain(kids).map("good_adj").compact().sum().value();

    let good_n =
      _.chain(kids)
        .filter((i) => i?.ymca2 !== null && i.good_adj > 0)
        .value()?.length ?? 0;
    let bad_n =
      _.chain(kids)
        .filter((i) => i?.ymca2 !== null && i.good_adj < 0)
        .value()?.length ?? 0;
    if (test_mode) console.log({ good_n, bad_n });
    let logi = (good_n ?? 0) / (kids_n || 1);
    if (test_mode) console.log("good/total", logi);
    let base_bonus = get_logi_bonus(logi);
    if (test_mode) console.log("base bonus", base_bonus);
    let diff = Math.max(0, good_n - bad_n * bad_fact);
    if (test_mode) console.log("diff", diff);
    let log_n = (diff <= 0 && 1) || (diff == 1 && 1.2) || diff;
    if (test_mode) console.log("log_n", log_n);
    let kids_n_bonus = Math.log(log_n) * log_fact;
    if (test_mode) console.log("kids_n bonus", kids_n_bonus);
    let added_bonus = Math.min(bonus_cap, base_bonus + kids_n_bonus);
    if (test_mode) console.log("added bonus", added_bonus);
    let br;
    if (adjs.length == 0) br = null;
    else {
      adjs = _.mean(adjs);
      br = adjs;
    }

    if (test_mode) console.log({ adjs });
    if (test_mode) console.log({ good_adjs });
    if (test_mode) console.log({ added_bonus });

    if (br !== null) {
      br += good_adjs;
    }
    if (!added_bonus || _.isNaN(added_bonus)) added_bonus = 0;
    br += added_bonus;

    let kg = {
      hid,
      odds: kids_scores_ob,
      avg,
      br,
      kids_n,
      // ymca2,
    };
    // console.log({ avg: dec2(avg), br: dec2(br) });
    if (test_mode)
      console.log(
        "# hid:",
        hid,
        "kids_n:",
        kids_n,
        "br:",
        dec(br, 2)
        // "ymca2:",
        // dec(ymca2, 2)
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
  test_mode = 1;
  for (let hid of hids) {
    let ob = await generate(hid);
    console.log(hid, ob);
  }
};

const all = async () => bulk.run_bulk_all(name, generate, coll, cs, test_mode);
const only = async (hids) =>
  bulk.run_bulk_only(name, generate, coll, hids, cs, test_mode);
const range = async (st, ed) =>
  bulk.run_bulk_range(name, generate, coll, st, ed, cs, test_mode);

const fixer1 = async () => {
  let fix_hids = [];
  let all_hids = await cyclic_depedency.get_all_hids();
  for (let chunk of _.chunk(all_hids, 5000)) {
    let [a, b] = [chunk[0], chunk[chunk.length - 1]];
    console.log("get", a, b);
    let hids = await zed_db.db
      .collection(coll)
      .find(
        { hid: { $in: chunk }, br: { $ne: null, $gt: 5 } },
        { projection: { hid: 1 } }
      )
      .toArray();
    hids = _.map(hids, "hid");
    console.log("GOT", hids.length);
    fix_hids = [...fix_hids, ...hids];
  }
  console.log(fix_hids.toString());

  await zed_db.db
    .collection(coll)
    .updateMany({ hid: { $in: fix_hids } }, { $set: { br: 1 } });

  await utils.delay(3000);
  await only(fix_hids);
  console.log("ENDED fixer");
};

const fixer2 = async () => {
  let flag = 2;
  let fix_hids = await zed_db.db
    .collection(coll)
    .find({ flag }, { projection: { hid: 1 } })
    .toArray();
  fix_hids = _.map(fix_hids, "hid");
  for (let hid of fix_hids) {
    await zed_db.db
      .collection(coll)
      .updateOne({ hid }, { $set: { br: 1, flag: flag + 1 } });
  }
  for (let hid of fix_hids) {
    await only([hid]);
  }
};

const fixer3 = async () => {
  let fix_hids = [];
  let all_hids = await cyclic_depedency.get_all_hids();
  for (let chunk of _.chunk(all_hids, 5000)) {
    let [a, b] = [chunk[0], chunk[chunk.length - 1]];
    console.log("get", a, b);
    let hids = await zed_db.db
      .collection(coll)
      .find(
        { hid: { $in: chunk }, br: { $ne: null, $gt: 5 } },
        { projection: { hid: 1 } }
      )
      .toArray();
    hids = _.map(hids, "hid");
    console.log("GOT", hids.length);
    fix_hids = [...fix_hids, ...hids];
  }
  console.log("\n--\nGOT fix_hids", fix_hids.length);
  let kids = await zed_db.db
    .collection("horse_details")
    .find({ hid: { $in: fix_hids } }, { projection: { offsprings: 1 } });
  kids = _.chain(kids).map("offsprings").flatten().value();
  let kids_docs = await zed_db.db
    .collection("horse_details")
    .find(
      { hid: { $in: kids } },
      {
        projection: {
          hid: 1,
          bloodline: 1,
          breed_type: 1,
          genotype: 1,
          parents: 1,
        },
      }
    )
    .toArray();

  let all_parents = [];
  let all_z_ids = [];
  for (let doc of kids_docs) {
    let { bloodline, breed_type, genotype } = doc;
    let ps = _.values(doc?.parents) ?? [];
    all_parents = [...all_parents, ...ps];
    let id = `${bloodline}-${breed_type}-${genotype}`;
    all_z_ids = [...all_z_ids, id];
  }

  all_parents = _.uniq(_.compact(all_parents));
  all_z_ids = _.uniq(_.compact(all_z_ids));

  console.log("kids:", kids.length);
  console.log("parents:", all_parents.length);
  console.log("z_ids:", all_z_ids.length);

  // console.log("Running YMCA2");
  // await ymca2_s.only(kids);

  // console.log("Running Z_IDs");
  // for (let id of all_z_ids) await ymca2_table.update_z_id_row(id);

  // console.log("Running Parents BR");
  // await zed_db.db
  //   .collection(coll)
  //   .updateMany({ hid: { $in: all_parents } }, { $set: { br: 1 } });
  // for (let hid of all_parents) await only([hid]);

  // console.log("Running Z_IDs");
  // for (let id of all_z_ids) await ymca2_table.update_z_id_row(id);
  // console.log("ENDED FIXER");
};
const fixer4 = async () => {
  let id = "Nakamoto-cross-Z19";
  await ymca2_table.update_z_id_row(id);
};

const fixer = fixer3;

const rating_breed = {
  generate,
  calc,
  test,
  all,
  only,
  range,
  fixer,
};
module.exports = rating_breed;
