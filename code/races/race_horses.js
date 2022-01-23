const _ = require("lodash");
const { zed_db } = require("../connection/mongo_connect");
const mega = require("../v3/mega");
const cron = require("node-cron");
const cron_parser = require("cron-parser");
const utils = require("../utils/utils");

const def_cs = 300;
const run_cs = 30;

const coll = "race_horses";
const push = async (ob) => {
  let { hid, date } = ob;
  let doc = await zed_db.db.collection(coll).findOne({ hid }, { date: 1 });
  if (!doc || date > doc.date)
    await zed_db.db
      .collection(coll)
      .updateOne({ hid }, { $set: ob }, { upsert: true });
};

const push_ar = async (ar) => {
  await Promise.all(ar.map(push));
};

const pull = async (cs = def_cs) => {
  let ar =
    (await zed_db.db
      .collection(coll)
      .find({}, { projection: { hid: 1, tc: 1 } })
      .sort({ date: -1 })
      .limit(cs)
      .toArray()) || [];
  return ar;
};

const update_horse_tc = async (doc) => {
  let { hid, tc } = doc || {};
  if (tc === undefined || tc === null) return;
  await zed_db.db
    .collection("horse_details")
    .updateOne({ hid }, { $set: { tc } });
};

const run = async (cs = def_cs) => {
  let ar = await pull(cs);
  if (_.isEmpty(ar)) {
    console.log("empty");
    return;
  }
  await Promise.all(ar.map(update_horse_tc));
  let hids = _.map(ar, "hid");
  for (let chunk_hids of _.chunk(hids, run_cs)) {
    await mega.only_w_parents_br(chunk_hids, run_cs);
  }
  console.log("updated", hids.length);
  await zed_db.db.collection(coll).deleteMany({ hid: { $in: hids } });
};

const run_cron = (cs = def_cs) => {
  let cron_str = "0 */1 * * * *";
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("Next run:", c_itvl.next().toISOString(), "\n");
  cron.schedule(cron_str, () => run(cs), utils.cron_conf);
};

const test = async () => {
  run(def_cs);
};

const race_horses = {
  push,
  push_ar,
  pull,
  test,
  run,
  run_cron,
};
module.exports = race_horses;
