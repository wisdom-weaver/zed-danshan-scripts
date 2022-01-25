const _ = require("lodash");
const { zed_db, zed_ch } = require("../connection/mongo_connect");
const global_req = require("../global_req/global_req");
const bulk = require("../utils/bulk");
const utils = require("../utils/utils");

let test_mode = 0;
let cs = 1;
let z_ALL;
let ymca2_avgs;
const name = "est_ymca";
const coll = "rating_breed3";

const get_reqs = () => {
  console.log("get_reqs");
  z_ALL = global_req.get_data().z_ALL;
  ymca2_avgs = global_req.get_data().ymca2_avgs;
};

const get_hid_est = async (hid) => {
  hid = parseFloat(hid);
  if (!hid || _.isNaN(hid)) return undefined;
  let { est_ymca = null } =
    (await zed_db.db
      .collection(coll)
      .findOne({ hid }, { projection: { est_ymca: 1 } })) ?? {};
  return est_ymca;
};

const calc = async ({ hid, hdoc }) => {
  try {
    if (_.isEmpty(hdoc)) {
      console.log("err", hid, "empty");
      return null;
    }
    if (!z_ALL || !ymca2_avgs) get_reqs();
    let est_ymca;
    let { bloodline, breed_type, genotype } = hdoc || {};
    let { mother = null, father = null } = hdoc?.parents || {};
    if (!mother || !father)
      return ymca2_avgs[`${bloodline}-${breed_type}-${genotype}`]?.avg;
    else {
      let [mey, fey] = await Promise.all([mother, father].map(get_hid_est));
      if (test_mode) console.log({ mey, fey });
      if (mey == null || fey === null) {
        if (test_mode) console.log("err", hid, "parents est_ymca=null");
        return null;
      }
      return ((mey + fey) / 2) * 0.95;
    }
    return est_ymca;
  } catch (err) {
    console.log("err in est_ymca", hid, err);
    return null;
  }
};

const generate = async (hid) => {
  try {
    hid = parseInt(hid);
    let hdoc = await zed_db.db
      .collection("horse_details")
      .findOne(
        { hid },
        { projection: { bloodline: 1, breed_type: 1, genotype: 1, parents: 1 } }
      );
    if (!hdoc) {
      console.log(hid, "details missing");
      return null;
    }
    if (test_mode) console.log(hdoc);
    const est_ymca = await calc({ hid, hdoc });
    let ob = { hid, est_ymca };
    return ob;
  } catch (err) {
    console.log("err in est_ymca", err);
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

const est_ymca = { calc, generate, test, all, only, range };
module.exports = est_ymca;
