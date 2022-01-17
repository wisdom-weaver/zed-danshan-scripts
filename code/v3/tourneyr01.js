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
const dur = 2.1 * 60 * 1000;

let t_st_date = "2022-01-17T18:00:00.000Z";

let stable_ob = [];
let all_hids = [];

const calc_horse_points = async (hid) => {
  hid = parseFloat(hid);
  let races =
    (await zed_ch.db
      .collection("zed")
      .find({ 2: { $gte: t_st_date }, 6: hid }, { projection: { 6: 1, 8: 1 } })
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
  console.log("init");
  stable_ob = await get_stable_ob();
  all_hids = await get_all_hids();
  console.log("stables:", stable_ob.length);
  console.log("stable_hids:", all_hids.length);
  await update_list();
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
    .find(
      { stable_name: { $ne: null } },
      { projection: { _id: 0, stable_name: 1, hids: 1 } }
    )
    .toArray();
  return ob;
};
const get_all_hids = async () => {
  if (!stable_ob) stable_ob = await get_stable_ob();
  let hids = _.map(stable_ob, "hids");
  hids = _.flatten(hids);
  return hids;
};

const get_stable_name = (hid) => {
  let ob = _.chain(stable_ob).keyBy("stable_name").mapValues("hids").value();
  for (let [stable, hids] of _.entries(ob)) {
    if (hids.includes(hid)) return stable;
  }
  return "na-stable";
};

const update_list = async () => {
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
  await Promise.all(miss.map(calc_horse_points));
};

const run_cron = () => {
  console.log("run_cron");
  let cron_str = "*/2 * * * *";
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("Next run:", c_itvl.next().toISOString(), "\n");
  let runner = now;
  cron.schedule(cron_str, runner, utils.cron_conf);
};
const test = async () => {
  update_list();
};
const main = () => {};
const tourneyr01 = { test, run_cron, now, run_dur };
module.exports = tourneyr01;
