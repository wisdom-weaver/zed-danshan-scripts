const { get_races_of_hid } = require("../utils/cyclic_dependency");
const _ = require("lodash");
const bulk = require("../utils/bulk");
const { zed_db } = require("../connection/mongo_connect");
const { options } = require("../utils/options");
const { geno, dec, get_fee_tag } = require("../utils/utils");
const race_utils = require("../utils/race_utils");
const cyclic_depedency = require("../utils/cyclic_dependency");
const coll = "rating_blood3";
const name = "base_ability v3";
let cs = 200;
let test_mode = 0;
const class_val = {
  1: 4,
  2: 3,
  3: 2,
  4: 1,
  5: 0.5,
  6: 0,
};
const c_ob = {
  0: 0,
  1: 5,
  2: 4,
  3: 3,
  4: 2,
  5: 1,
};
const f_ob = {
  A: 5,
  B: 5,
  C: 4,
  D: 3,
  E: 2,
  F: 1,
};
const p_ob = {
  1: 5,
  2: 4,
  3: 3,
  4: 2,
  5: 1,
  6: 0.5,
};
let pos_pts = {
  1: 0.6,
  2: 0.5,
  3: 0.4,
  4: 0.3,
  5: 0.2,
  6: 0.1,
  7: 0.1,
  8: 0.2,
  9: 0.3,
  10: 0.4,
  11: 0.5,
  12: 0.6,
};
const flame_ob = 3;
const min_ratio = 1.1;
const min_races = 2;
const classes = [1, 2, 3, 4, 5, 6];
const lc = classes[classes.length - 1];

const get_class_ratio_ob = ({ c, dist_races }) => {
  let c_races = race_utils.filter_races(dist_races, { c });
  let p_count = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12].map((p) => {
    let n = race_utils.filter_races(c_races, { position: p });
    n = n?.length || 0;
    return [p, n];
  });
  p_count = _.fromPairs(p_count);
  let left = _.sum([1, 2, 3, 4, 5, 6].map((p) => p_count[p]));
  let right = _.sum([7, 8, 9, 10, 11, 12].map((p) => p_count[p]));
  // let ob = { c, tot: c_races.length, left, right };
  let left_pts = _.sum([1, 2, 3, 4, 5, 6].map((p) => p_count[p] * pos_pts[p]));
  if (_.isNaN(left_pts)) left_pts = 0;
  let right_pts = _.sum(
    [7, 8, 9, 10, 11, 12].map((p) => p_count[p] * pos_pts[p])
  );
  if (_.isNaN(right_pts)) right_pts = 0;
  let ratio = (left_pts || 0) / (right_pts || 1);
  let act_ratio = ratio;
  if (left + right < min_races) ratio = null;
  if (ratio == 0) ratio = null;
  if (ratio < min_ratio) ratio = null;
  let ob = { c, left, right, left_pts, right_pts, act_ratio, ratio };
  // if (test_mode) console.log(ob);
  return ob;
};

const pick_c = (ratio_ar) => {
  ratio_ar = _.keyBy(ratio_ar, "c");
  let ratio_ob = _.chain(ratio_ar)
    .keyBy("c")
    .mapValues((i) => {
      if (i.left + i.right < min_races) return null;
      return i["ratio"];
    })
    .value();
  for (let c of classes) {
    let difft = ratio_ob[c] - ratio_ob[c - 1 < 1 ? 1 : c - 1];
    let diffb = -(ratio_ob[c] - ratio_ob[c + 1 > lc ? lc : c + 1]);
    let tb_diff = Math.abs(difft + diffb);
    let tot_diff = Math.abs(difft) + Math.abs(diffb);
    if (ratio_ob[c] == null) tot_diff = 0;
    if (c == 1 || c == lc) tot_diff *= 2;
    ratio_ar[c] = { ...ratio_ar[c], difft, diffb, tot_diff, tb_diff };
  }
  // console.table(ratio_ar);
  const mean_diff = _.mean(_.compact(_.map(_.values(ratio_ar), "tot_diff")));
  if (test_mode) console.log({ mean_diff });
  ratio_ar = classes.map((i) => {
    let { difft, diffb, tot_diff, tb_diff, ratio } = ratio_ar[i];
    let onlys = _.filter(classes, (e) => e !== i);
    onlys = _.compact(onlys.map((ec) => ratio_ar[ec].tot_diff || null));
    let ex_mean = _.mean(onlys);
    if (_.sum(onlys) == 0) ex_mean = mean_diff;
    let ex_ratio = mean_diff / ex_mean;

    let range_pm = 0.25;
    let bullshit;
    if (i.left + i.right < 15) {
      !_.inRange(ex_ratio, 1 - range_pm, 1 + range_pm);
      if (!bullshit) {
        let rat2 = ratio_ar[i + 1]?.ratio;
        if (i < lc && rat2 && rat2 < ratio_ar[i].ratio) bullshit = true;
      }
    } else bullshit = false;
    let pick = ratio != null && ratio != 0 && !bullshit;
    return {
      ...ratio_ar[i],
      difft: dec(difft),
      diffb: dec(diffb),
      tot_diff: dec(tot_diff),
      tb_diff: dec(tb_diff),
      ex_mean: dec(ex_mean),
      ex_ratio: dec(ex_ratio),
      bullshit,
      pick,
    };
  });
  for (let c of classes) {
    let ref1 = ratio_ar[c - 1];
    if (!ref1.pick || ref1.left + ref1.right >= 15) {
      ref1.pick2 = ref1.pick;
      continue;
    }
    if (c == lc) ref1.pick2 = ref1.pick;
    for (let i = c + 1; i <= lc; i++) {
      let ref2 = ratio_ar[i - 1];
      if (!ref2.pick) {
        if (i == lc) ref1.pick2 = ref1.pick;
        continue;
      }
      if (ref2.ratio < ref1.ratio) ref1.pick2 = false;
      else {
        ref1.pick2 = true;
        break;
      }
    }
  }
  let pick2 = _.find(ratio_ar, { pick2: true })?.c || null;
  if (test_mode) {
    console.table(ratio_ar);
    console.log({ pick2 });
  }
  if (!pick2) return null;
  let ob = _.find(ratio_ar, { c: pick2 }) || {};
  return { c: pick2, ...ob };
};
const pick_avg_fee = ({ c, races }) => {
  let c_races = race_utils.filter_races(races, { c }, { paid: 1 });
  let avg_fee = _.meanBy(c_races, "entryfee_usd");
  if (!avg_fee || _.isNaN(avg_fee)) return 0;
  // let avg_fee_tag = get_fee_tag(avg_fee);
  if (test_mode) console.log({ avg_fee });
  return avg_fee;
};

const diff = () => {};

const calc = async ({ hid, races = [], tc, hdoc }) => {
  try {
    hid = parseInt(hid);
    let dist_races = race_utils.filter_races(
      races,
      { d: [1400, 1600, 1800] },
      { no_tourney: 1 }
      // { no_tourney: 1, paid: 1 }
    );
    if (test_mode)
      console.log("#", hid, { n: races.length, dist_n: dist_races.length });
    let ratio_ar = [];

    for (let c of classes) {
      let ratio_ob = get_class_ratio_ob({ c, dist_races });
      ratio_ar.push(ratio_ob);
    }
    const c_ob = pick_c(ratio_ar);
    if (!c_ob)
      return {
        hid,
        base_ability: { c: null, ratio: null, avg_fee: null, n: null },
      };
    let { c, left, right, ratio } = c_ob;
    if (test_mode) console.table([c_ob]);
    let avg_fee = pick_avg_fee({ c, races: dist_races });
    let c_races_n = left + right;
    if (!c_races_n || _.isNaN(c_races_n)) c_races_n = 0;
    let conf = Math.min(99, c_races_n * 2);
    let base = class_val[c] + avg_fee * 0.01 + ratio * 0.05;

    let n;
    if (_.isNaN(base) || base == null) {
      n = null;
    } else {
      let avg_ob = await cyclic_depedency.get_ymca_avgs(hdoc);
      let avg_base = avg_ob?.avg_base;
      let high = null; // let high = avg_base * 1.25;
      let low = null; // let low = avg_base * 0.75;
      let adjuster = Math.random() * 54 * 0.001;
      let final_base = base;
      // if (base > low && base < high) final_base = base;
      // else final_base = base > high ? high * 1.098 : low * 0.904;
      final_base += adjuster;
      n = final_base;
      if (test_mode)
        console.log({ base, avg_base, low, high, adjuster, final_base });
    }

    return { hid, base_ability: { c, ratio, avg_fee, c_races_n, conf, n } };
  } catch (err) {
    console.log("err on rating", hid);
    console.log(err);
  }
};
const generate = async (hid) => {
  hid = parseInt(hid);
  let hdoc = await zed_db.db
    .collection("horse_details")
    .findOne(
      { hid },
      { projection: { tc: 1, bloodline: 1, breed_type: 1, genotype: 1 } }
    );
  let races = await get_races_of_hid(hid);
  if (test_mode) console.log("races.len:", races.length);
  let tc = hdoc?.tc;
  let ob = await calc({ races, tc, hid, hdoc });
  if (test_mode) console.log(hid, ob, "\n\n");
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

const test = async (hids) => {
  test_mode = 1;
  if (test_mode) cs = 1;
  for (let hid of hids) generate(hid);
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
