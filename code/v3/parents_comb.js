const _ = require("lodash");
const { zed_db } = require("../connection/mongo_connect");
const bulk = require("../utils/bulk");

let test_mode = 0;
let cs = 500;
const name = "parents_comb";
const coll = "rating_breed3";

const calc = async ({ hid, hdoc }) => {
  try {
    if (_.isEmpty(hdoc)) return null;
    let { parents } = hdoc;
    if (_.isEmpty(parents)) return null;
    let { mother, father } = parents;
    if (test_mode) console.log(hid, parents);
    if (!mother && !father) return { hid, comb_score: null };
    let { br: f_br = 0 } = await zed_db.db
      .collection("rating_breed2")
      .findOne({ hid: father }, { br: 1 });

    let { br: m_br = 0 } = await zed_db.db
      .collection("rating_breed2")
      .findOne({ hid: mother }, { br: 1 });

    let comb_score = _.sum([f_br, m_br]);
    if (test_mode) console.log(hid, "comb_score", comb_score);
    let ob = { hid, comb_score };
    return ob;
  } catch (err) {
    console.log(`err in ${name}`, hid, err);
    return null;
  }
};

const generate = async (hid) => {
  try {
    hid = parseInt(hid);
    let hdoc = await zed_db.db
      .collection("horse_details")
      .findOne({ hid }, { parents: 1 });
    if (!hdoc) {
      console.log(hid, "hdoc missing");
      return null;
    }
    let ob = await calc({ hid, hdoc });
    return ob;
  } catch (err) {
    console.log(`err in ${name}`, err);
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

const parents_comb = { calc, generate, test, all, only, range };
module.exports = parents_comb;
