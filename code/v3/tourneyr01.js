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
  let traces_n = poss.length;
  let ob = { hid, pts, avg, traces_n };
  console.log(ob);
  await zed_db.db
    .collection(coll2)
    .updateOne({ hid }, { $set: ob }, { upsert: true });
};

const run_dur = async ([st, ed]) => {
  console.log("run_dur", [st, ed]);
  st = iso(st);
  ed = iso(ed);
  let races = await zed_ch.db
    .collection("zed")
    .find({ 2: { $gte: st, $lte: ed } }, { projection: { 4: 1, 6: 1, 8: 1 } })
    .toArray();
  console.log("docs.len", races.len);
  races = _.groupBy(races, 4);
  for (let [rid, race] of _.entries(races)) {
    race = _.sortBy(race, (i) => parseFloat(i[8]));
    let c = race[0][5];
    let hids = _.map(race, 6);
    let top3 = hids.slice(0, 3);
    console.log(rid, `C${c}`, race.length, "top3:", top3);
    await Promise.all(top3.map((h) => calc_horse_points(h)));
  }
};
const now = async () => {
  let ed = moment().subtract(2, "minutes").toISOString();
  let st = moment(new Date(nano(ed) - dur)).toISOString();
  await run_dur([st, ed]);
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
  now();
};
const main = () => {};
const tourneyr01 = { test, run_cron, now, run_dur };
module.exports = tourneyr01;
