const _ = require("lodash");
const { zed_ch, zed_db } = require("../connection/mongo_connect");
const { options } = require("../utils/options");
const utils = require("../utils/utils");
const { geno, dec } = require("../utils/utils");

const coll = "rating_breed3";
let doc_id = "ymca2-global-avgs";

let ymca2_avgs;

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
  return ymca2_avgs[id];
};
const get_z_table_for_id_v1 = async (id) => {
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
    .collection(coll)
    .find({ hid: { $in: hids } }, { projection: { _id: 0, ymca2: 1, br: 1 } })
    .toArray();

  let scores = _.chain(docs).map("ymca2").compact().value();
  let brs = _.chain(docs)
    .map("br")
    .filter((i) => i !== Infinity)
    .compact()
    .value();

  let y_avg = _.mean(scores);
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

  console.log(id);
  return {
    count_all: ar.length,
    count: scores.length,
    count_: brs.length,
    y_avg,
    y_min,
    y_max,
    br_min,
    br_avg,
    br_max,
  };
};
const get_z_table_for_id_v2 = async (id) => {
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
    .collection(coll)
    .find({ hid: { $in: hids } }, { projection: { _id: 0, ymca2: 1, br: 1 } })
    .toArray();

  let scores = _.chain(docs).map("ymca2").compact().value();
  let filt_scores = utils.remove_bullshits(scores);
  // console.log("all__scores.length", scores.length);
  // console.log("filt_scores.length", filt_scores.length);

  let brs = _.chain(docs)
    .map("br")
    .filter((i) => i !== Infinity)
    .compact()
    .value();

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
  console.log(id, avg);

  return {
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
  };
};
const get_z_table_for_id = get_z_table_for_id_v2;

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
  console.table(ob);
  await zed_db.db
    .collection("requirements")
    .updateOne(
      { id: doc_id },
      { $set: { id: doc_id, avg_ob: ob } },
      { upsert: true }
    );
  console.log("done");
};

const get = async (print = 0) => {
  let doc = await zed_db.db.collection("requirements").findOne({ id: doc_id });
  let avgs = doc?.avg_ob || null;
  console.log("GOT", "ymca2_avg");
  if (print) console.table(avgs);
  return avgs;
};

const test = async () => {
  // let ob = await get();
  // let keys = [];
  // for (let [bl_idx, bl] of _.entries(bloodlines)) {
  //   for (let [bt_idx, bt] of _.entries(breed_types)) {
  //     let id_st = `${bl}-${bt}`;
  //     let [z_mi, z_mx] = z_mi_mx[id_st];
  //     for (let z = z_mi; z <= z_mx; z++) {
  //       keys.push(`${bl}-${bt}-Z${z}`);
  //     }
  //   }
  // }
  // ob = _.entries(ob).map(([id, e], idx) => {
  //   return [keys[idx], { ...e, avg: e.y_avg, base: null }];
  // });
  // console.log(ob.length);
  // ob = _.fromPairs(ob);
  // // console.table(ob);
  // console.log(keys.length);
  // await zed_db.db
  //   .collection("requirements")
  //   .updateOne(
  //     { id: doc_id },
  //     { $set: { id: doc_id, avg_ob: ob } },
  //     { upsert: true }
  //   );
  // await zed_db.db
  //   .collection("rating_breed3")
  //   .updateMany({}, { $set: { br: 1 } });
  console.log("done");
};

const generate = generate_v2;

const ymca2_table = { generate, get, test };
module.exports = ymca2_table;
