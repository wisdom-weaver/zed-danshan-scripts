const _ = require("lodash");
const { zed_ch, zed_db } = require("../connection/mongo_connect");
const { options } = require("../utils/options");
const { geno, dec } = require("../utils/utils");

const coll = "rating_breed3";
// let doc_id = "ymca2-global-avgs";
let doc_id = "kid-score-global"

const get_z_table_for_id = async (id) => {
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

  let avg = _.mean(scores);
  if (!avg || _.isNaN(avg)) avg = null;
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

  let str = [y_min, avg, y_max, br_min, br_avg, br_max]
    .map((e) => dec(e))
    .join(" ");

  console.log({ id, count: scores.length }, str);
  return {
    count_all: ar.length,
    count: scores.length,
    count_: brs.length,
    avg,
    y_min,
    y_max,
    br_min,
    br_avg,
    br_max,
  };
};
const generate = async () => {
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
    ob[id] = await get_z_table_for_id(id);
  }
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

const get = async (print=0) => {
  let doc = await zed_db.db.collection("requirements").findOne({ id: doc_id });
  let avgs = doc?.avg_ob || null;
  console.log("GOT", "ymca2_avg");
  if (print) console.table(avgs);
  return avgs;
};

const ymca2_table = { generate, get };

module.exports = ymca2_table;
