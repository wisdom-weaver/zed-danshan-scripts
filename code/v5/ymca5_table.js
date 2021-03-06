const _ = require("lodash");
const { zed_ch, zed_db } = require("../connection/mongo_connect");
const cyclic_depedency = require("../utils/cyclic_dependency");
const { options } = require("../utils/options");
const utils = require("../utils/utils");
const { geno, dec } = require("../utils/utils");
const cron = require("node-cron");

const coll_br = "rating_breed5";
const coll_y = "ymca5";
let doc_id = "ymca5-global-avgs";

let ymca5_avgs;

const bloodlines = ["Nakamoto", "Szabo", "Finney", "Buterin"];
const breed_types = [
  "genesis",
  "legendary",
  "exclusive",
  "elite",
  "cross",
  "pacer",
];
const z_mi_mx = {
  "Nakamoto-genesis": [1, 2],
  "Nakamoto-legendary": [2, 4],
  "Nakamoto-exclusive": [3, 268],
  "Nakamoto-elite": [4, 268],
  "Nakamoto-cross": [5, 268],
  "Nakamoto-pacer": [7, 268],
  "Szabo-genesis": [3, 4],
  "Szabo-legendary": [4, 8],
  "Szabo-exclusive": [5, 268],
  "Szabo-elite": [6, 268],
  "Szabo-cross": [7, 268],
  "Szabo-pacer": [8, 268],
  "Finney-genesis": [5, 7],
  "Finney-legendary": [6, 14],
  "Finney-exclusive": [7, 268],
  "Finney-elite": [8, 268],
  "Finney-cross": [9, 268],
  "Finney-pacer": [10, 268],
  "Buterin-genesis": [8, 10],
  "Buterin-legendary": [9, 20],
  "Buterin-exclusive": [10, 268],
  "Buterin-elite": [11, 268],
  "Buterin-cross": [12, 268],
  "Buterin-pacer": [13, 268],
};

const get_z_table_for_id_bkp = async (id) => {
  return ymca5_avgs[id];
};

const get_z_table_for_id_v2 = async (id) => {
  try {
    let [bl, bt, z] = id.split("-");
    if (!bl || !bt || !z) return null;
    let ar = await zed_db.db
      .collection("horse_details")
      .find(
        {
          bloodline: bl,
          breed_type: bt,
          genotype: z,
        },
        { projection: { _id: 0, hid: 1, tc: 1 } }
      )
      .toArray();
    let hids = _.map(ar, "hid") || [];
    let tcs = _.map(ar, "tc") || [];
    let avg_tc = _.mean(
      _.filter(tcs, (e) => !_.isNaN(e) && ![null, undefined].includes(e))
    );

    let docs2 = await zed_db
      .collection("rating_blood3")
      .find(
        { hid: { $in: hids } },
        { projection: { _id: 0, "base_ability.n": 1 } }
      )
      .toArray();
    let bases = _.map(docs2, (e) => e?.base_ability?.n || null);
    bases = _.filter(bases, (e) => ![null, undefined, NaN].includes(e));

    let ymca5_docs = await zed_db.db
      .collection(coll_y)
      .find({ hid: { $in: hids } }, { projection: { _id: 0, ymca5: 1 } })
      .toArray();

    let scores = _.chain(ymca5_docs).map("ymca5").compact().value();
    let filt_scores = utils.remove_bullshits(scores);
    // console.log("all__scores.length", scores.length);
    // console.log("filt_scores.length", filt_scores.length);

    let br_docs = await zed_db.db
      .collection(coll_br)
      .find({ hid: { $in: hids } }, { projection: { _id: 0, br: 1 } })
      .toArray();
    let brs = _.chain(br_docs)
      .map("br")
      .filter((i) => i !== Infinity)
      .compact()
      .value();

    let dps = await zed_db.db
      .collection("dp4")
      .find({ hid: { $in: hids } }, { projection: { dp: 1, _id: 0 } })
      .toArray();
    dps = _.map(dps, "dp");

    let rngs = await zed_db.db
      .collection("gap4")
      .find({ hid: { $in: hids } }, { projection: { gap: 1, _id: 0 } })
      .toArray();
    rngs = _.map(rngs, "gap");

    let y_avg = _.mean(filt_scores);
    if (!y_avg || _.isNaN(y_avg)) y_avg = _.mean(scores);
    if (!y_avg || _.isNaN(y_avg)) y_avg = null;

    let y_min = _.min(scores);
    if (!y_min || _.isNaN(y_min)) y_min = null;
    let y_max = _.max(scores);
    if (!y_max || _.isNaN(y_max)) y_max = null;

    let br_avg = _.mean(brs);
    if (!br_avg || _.isNaN(br_avg)) br_avg = null;
    let br_min = _.min(brs);
    if (!br_min || _.isNaN(br_min)) br_min = null;
    let br_max = _.max(brs);
    if (!br_max || _.isNaN(br_max)) br_max = null;

    let avg = y_avg;
    // console.log(id, avg);
    console.log(id);

    let dp_avg = _.mean(dps);
    if (!dp_avg || _.isNaN(dp_avg)) dp_avg = null;
    let dp_min = _.min(dps);
    if (!dp_min || _.isNaN(dp_min)) dp_min = null;
    let dp_max = _.max(dps);
    if (!dp_max || _.isNaN(dp_max)) dp_max = null;

    let rng_avg = _.mean(rngs);
    if (!rng_avg || _.isNaN(rng_avg)) rng_avg = null;
    let rng_min = _.min(rngs);
    if (!rng_min || _.isNaN(rng_min)) rng_min = null;
    let rng_max = _.max(rngs);
    if (!rng_max || _.isNaN(rng_max)) rng_max = null;

    let avg_base = _.mean(bases);
    if (!avg_base || _.isNaN(avg_base)) avg_base = null;
    let base_min = _.min(bases);
    if (!base_min || _.isNaN(base_min)) base_min = null;
    let base_max = _.max(bases);
    if (!base_max || _.isNaN(base_max)) base_max = null;

    let ob = {
      count_all: ar.length,
      count: scores.length,
      count_: brs.length,
      base: null,
      avg,
      y_avg,
      y_min,
      y_max,
      br_min,
      br_avg,
      br_max,
      avg_tc,
      avg_base,
      base_min,
      base_max,
      dp_avg,
      dp_min,
      dp_max,
      rng_avg,
      rng_min,
      rng_max,
    };
    // console.log(ob);
    return ob;
  } catch (err) {
    console.log("err at get_z_table_for_id_v2", err);
    return null;
  }
};
const get_z_table_for_id = get_z_table_for_id_v2;

const get_z_table_for_id_avg_base = async (id) => {
  let [bl, bt, z] = id.split("-");
  if (!bl || !bt || !z) return null;
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
  let docs2 = await zed_db
    .collection("rating_blood3")
    .find(
      { hid: { $in: hids } },
      { projection: { _id: 0, "base_ability.n": 1 } }
    )
    .toArray();
  let bases = _.map(docs2, (e) => e?.base_ability?.n || null);
  let avg_base = _.mean(
    _.filter(bases, (e) => ![null, undefined, NaN].includes(e))
  );
  return avg_base;
};
const update_z_id_avg_base = async (id) => {
  let avg_base = await get_z_table_for_id_avg_base(id);
  console.log(id, { avg_base });
  await zed_db.db
    .collection("requirements")
    .updateOne(
      { id: doc_id },
      { $set: { [`avg_ob.${id}.avg_base`]: avg_base } }
    );
};
const update_z_id_row = async (id) => {
  let ob = await get_z_table_for_id_v2(id);
  if (_.isEmpty(ob)) {
    console.log("err update_z_is_row");
    return;
  }
  // console.log(id, ob);
  await zed_db.db
    .collection("requirements")
    .updateOne(
      { id: doc_id },
      { $set: { [`avg_ob.${id}`]: ob } },
      { upsert: true }
    );
};

const generate_v1 = async () => {
  let ob = {};
  let keys = [];

  for (let [bl_idx, bl] of _.entries(bloodlines)) {
    for (let [bt_idx, bt] of _.entries(breed_types)) {
      let id_st = `${bl}-${bt}`;
      let [z_mi, z_mx] = z_mi_mx[id_st];
      for (let z = z_mi; z <= z_mx; z++) {
        let id = `${bl}-${bt}-Z${z}`;
        let id_ob = await get_z_table_for_id(id);
        y_avg = id_ob?.y_avg;
        if (bt == "genesis" && z == z_mi) {
          ob[id] = { base: y_avg, y_avg };
        } else if (z == z_mi) {
          let prev_id = `${bl}-${breed_types[bt_idx - 1]}-Z${z}`;
          let prev = ob[prev_id];
          ob[id] = { base: prev.base * 0.85, y_avg };
        } else {
          let prev_id = `${bl}-${bt}-Z${z - 1}`;
          let prev = ob[prev_id];
          ob[id] = { base: prev.base * 0.95, y_avg };
        }
        ob[id] = { ...id_ob, ...ob[id] };
      }
    }
  }

  for (let [id, { base, y_avg, count_all }] of _.entries(ob)) {
    let final;
    if (count_all < 50) {
      if (y_avg == null) final = base;
      else if (_.inRange(y_avg, base * 0.9, base * 1.1 + 1e-14)) final = y_avg;
      else if (y_avg < base * 0.9) final = base * 0.9;
      else if (y_avg > base * 1.1) final = base * 1.1;
    } else final = y_avg;
    ob[id].avg = final;
  }
  // console.table(ob);
  // return;
  await zed_db.db
    .collection("requirements")
    .updateOne(
      { id: doc_id },
      { $set: { id: doc_id, avg_ob: ob } },
      { upsert: true }
    );
  console.log("done");
};
const generate_v2 = async () => {
  let ob = {};
  for (let [bl_idx, bl] of _.entries(bloodlines)) {
    for (let [bt_idx, bt] of _.entries(breed_types)) {
      let id_st = `${bl}-${bt}`;
      let [z_mi, z_mx] = z_mi_mx[id_st];
      for (let z = z_mi; z <= z_mx; z++) {
        let id = `${bl}-${bt}-Z${z}`;
        let id_ob = await get_z_table_for_id(id);
        ob[id] = { ...id_ob };
      }
    }
  }
  // console.table(ob);
  await zed_db.db
    .collection("requirements")
    .updateOne(
      { id: doc_id },
      { $set: { id: doc_id, avg_ob: ob } },
      { upsert: true }
    );
  console.log("done");
  return ob;
};

const get = async (print = 0) => {
  let doc = await zed_db.db.collection("requirements").findOne({ id: doc_id });
  let avgs = doc?.avg_ob || null;
  console.log("GOT", "ymca5_avg");
  if (print) console.table(avgs);
  return avgs;
};

const test = async () => {
  let i = 0;
  for (let [bl_idx, bl] of _.entries(bloodlines)) {
    for (let [bt_idx, bt] of _.entries(breed_types)) {
      let id_st = `${bl}-${bt}`;
      let [z_mi, z_mx] = z_mi_mx[id_st];
      for (let z = z_mi; z <= z_mx; z++) {
        let id = `${bl}-${bt}-Z${z}`;
        await update_z_id_row(id);
        if (++i == 15) return;
        // return;
      }
    }
  }
  console.log("done");
};

const generate = generate_v2;

const run_cron = async () => {
  const runner = () => generate();
  const cron_str = `0 0 * * 1,4`;
  cyclic_depedency.print_cron_details(cron_str);
  cron.schedule(cron_str, async () => {
    console.log("started", iso());
    await runner();
    console.log("ended", iso());
  });
};

const ymca5_table = { generate, get, test, update_z_id_row, run_cron };
module.exports = ymca5_table;
