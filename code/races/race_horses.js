const moment = require("moment");
const _ = require("lodash");
const { zed_db } = require("../connection/mongo_connect");
const mega = require("../v3/mega");
const cron = require("node-cron");
const cron_parser = require("cron-parser");
const utils = require("../utils/utils");
const cyclic_depedency = require("../utils/cyclic_dependency");

const def_cs = 400;
const run_cs = 20;

const coll = "race_horses";
const push = async (ob) => {
  let { hid, date } = ob;
  ob.processing = 0;
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
  let st1 = moment().subtract("4", "minutes").toISOString();
  let st2 = moment().subtract("5", "minutes").toISOString();
  let st3 = moment().subtract("3", "hours").toISOString();
  let ar =
    (await zed_db.db
      .collection(coll)
      .find(
        {
          $or: [
            { date: { $lte: st1 }, processing: 0 },
            { date: { $lte: st2, $gte: st3 } },
          ],
        },
        { projection: { hid: 1, tc: 1, date: 1 } }
      )
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
  console.table(ar);
  if (_.isEmpty(ar)) {
    console.log("empty");
    return;
  }
  await Promise.all(ar.map(update_horse_tc));
  let hids = _.map(ar, "hid");
  let dates = _.chain(ar).map("date").value() ?? [];
  console.log("dates:", dates[0], dates[dates.length - 1], hids.length);
  console.log("hids:", hids.join(", "));
  await zed_db.db
    .collection(coll)
    .updateMany({ hid: { $in: hids } }, { $set: { processing: 1 } });
  for (let chunk_hids of _.chunk(hids, run_cs)) {
    await mega.only_w_parents_br(chunk_hids, run_cs);
  }
  console.log("updated", hids.length, "\n--\n\n");
  await zed_db.db.collection(coll).deleteMany({ hid: { $in: hids } });
};

const run_cron = (cs = def_cs) => {
  let cron_str = "0 * * * * *";
  cyclic_depedency.print_cron_details(cron_str);
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
