const _ = require("lodash");
const moment = require("moment");
const cron = require("node-cron");
const cron_parser = require("cron-parser");
const { zed_ch, zed_db } = require("../connection/mongo_connect");
const cyclic_depedency = require("../utils/cyclic_dependency");
const { nano, iso } = require("../utils/utils");
const utils = require("../utils/utils");

const coll = "tourneyr01";
const coll2 = "tourneyr01_leader";
const dur = 2.2 * 60 * 1000;

let t_st_date = "2022-01-19T00:00:00.000Z";
let t_ed_date = "2022-01-26T00:00:00.000Z";

let stable_ob = [];
let all_hids = [];
let unpaid_hids = [];

const calc_horse_points = async (hid) => {
  hid = parseFloat(hid);
  let races =
    (await zed_ch.db
      .collection("zed")
      .find(
        { 2: { $gte: t_st_date, $lte: t_ed_date }, 6: hid },
        { projection: { 6: 1, 8: 1 } }
      )
      .toArray()) ?? [];
  let poss = _.map(races, (i) => parseFloat(i[8]));
  let pts = poss.reduce((acc, e) => (acc + [1, 2, 3].includes(e) ? 1 : 0), 0);
  let avg = pts / poss.length;
  if (poss.length == 0) avg = 0;
  let traces_n = poss.length;
  let stable_name = get_stable_name(hid);
  let ob = { hid, pts, avg, traces_n, stable_name };
  console.log(`::${hid} #${traces_n}`, { avg, pts });
  await zed_db.db
    .collection(coll2)
    .updateOne({ hid }, { $set: ob }, { upsert: true });
};

const run_dur = async ([st, ed]) => {
  await init_run();
  console.log("run_dur", [st, ed]);
  st = iso(st);
  ed = iso(ed);
  let races = await zed_ch.db
    .collection("zed")
    .find(
      { 2: { $gte: st, $lte: ed } },
      { projection: { 4: 1, 6: 1, 8: 1, 5: 1 } }
    )
    .toArray();
  console.log("docs.len", races.length);
  races = _.groupBy(races, 4);
  for (let [rid, race] of _.entries(races)) {
    race = _.sortBy(race, (i) => parseFloat(i[8]));
    let c = race[0][5];
    let hids = _.map(race, 6);
    let top3 = hids.slice(0, 3);
    console.log(rid, `C${c}`, race.length, "top3:", top3);
    let to_eval = top3.filter((h) => all_hids.includes(h));
    await Promise.all(to_eval.map((h) => calc_horse_points(h)));
  }
};
const now = async () => {
  let ed = moment().subtract(2, "minutes").toISOString();
  let st = moment(new Date(nano(ed) - dur)).toISOString();
  await run_dur([st, ed]);
};

const get_stable_ob = async () => {
  let ob = await zed_db.db
    .collection(coll)
    .find({ stable_name: { $ne: null } })
    .toArray();
  return ob;
};
const update_hids_list = async () => {
  if (!stable_ob) stable_ob = await get_stable_ob();
  let hids = [];
  let un = [];
  for (let { payments } of stable_ob) {
    for (let p of payments) {
      if (p.status == "paid") {
        hids.push(p?.add_hids ?? []);
      } else {
        un.push(p?.add_hids ?? []);
      }
    }
  }
  all_hids = _.compact(_.flatten(hids));
  unpaid_hids = _.compact(_.flatten(un));
};

const get_stable_name = (hid) => {
  let ob = _.chain(stable_ob).keyBy("stable_name").mapValues("hids").value();
  for (let [stable, hids] of _.entries(ob)) {
    if (hids.includes(hid)) return stable;
  }
  return "na-stable";
};

const update_hid_docs = async () => {
  let ob = { id: "master", hids: all_hids };
  await zed_db.db
    .collection(coll2)
    .updateOne({ id: "master" }, { $set: ob }, { upsert: true });
  let hids_exists = await zed_db.db
    .collection(coll2)
    .find({ hid: { $in: all_hids } }, { projection: { hid: 1, _id: 0 } })
    .toArray();
  hids_exists = _.map(hids_exists, "hid");
  let miss = _.difference(all_hids, hids_exists);
  if (!_.isEmpty(miss)) await Promise.all(miss.map(calc_horse_points));
  if (!_.isEmpty(unpaid_hids))
    await zed_db.db.collection(coll2).deleteMany({ hid: { $in: unpaid_hids } });
};

const init_run = async () => {
  console.log("init");
  stable_ob = await get_stable_ob();
  await update_hids_list();
  console.log("stables:", stable_ob.length);
  console.log("paid  :", all_hids.length);
  console.log("unpaid:", unpaid_hids.length);
  await update_hid_docs();
};

const run_cron = () => {
  console.log("run_cron");
  let cron_str = "*/2 * * * *";
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("Next run:", c_itvl.next().toISOString(), "\n");
  let runner = now;
  cron.schedule(cron_str, runner, utils.cron_conf);
};

const now_h = () => {
  await init_run();
  for (let chu of _.chunk(all_hids, 25)) {
    await Promise.all(chu.map(calc_horse_points));
  }
};

const run_cron_h = () => {
  console.log("run_cron_h");
  let cron_str = "*/5 * * * *";
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("Next run:", c_itvl.next().toISOString(), "\n");
  let runner = now_h;
  cron.schedule(cron_str, runner, utils.cron_conf);
};
const test = async () => {
  // let ar = await zed_db.db
  //   .collection(coll)
  //   .find({ stable_name: { $ne: null } })
  //   .toArray();
  // for (let { stable_name, hids, payments } of ar) {
  //   let add_hids = hids;
  //   let id = `payments.${0}.add_hids`;
  //   await zed_db.db.collection(coll).updateOne(
  //     { stable_name },
  //     {
  //       $set: { [id]: add_hids },
  //     },
  //     { upsert: true }
  //   );
  // }
  console.log("done");
};
const main = () => {};
const tourneyr01 = { test, run_cron, now, run_dur, now_h, run_cron_h };
module.exports = tourneyr01;
