const _ = require("lodash");
const moment = require("moment");
const { zed_ch, zed_db } = require("../connection/mongo_connect");
const cyclic_depedency = require("../utils/cyclic_dependency");
const { nano } = require("../utils/utils");
const utils = require("../utils/utils");

const coll = "tourneyr01";
const dur = 2 * 60 * 1000;

let t_st_date = "2022-01-17T18:00:00.000Z";

const calc_horse_points = async (hid) => {
  hid = parseFloat(hid);
  let races = await zed_ch.db
    .collection("zed")
    .find({ 2: { $gte: t_st_date }, 6: hid }, { projection: { 6: 1, 8: 1 } })
    .toArray();
  let poss = _.map(races, i=>parseFloat(i[8]));
  console.log(hid, poss);
};

const run_dur = async ([st, ed]) => {
  console.log("run_dur", [st, ed]);
  let races = await zed_ch.db
    .collection("zed")
    .find({ 2: { $gte: st, $lte: ed } }, { projection: { 4: 1, 6: 1, 8: 1 } })
    .toArray();
  console.log("docs.len", races.len);
  races = _.groupBy(races, 4);
  for (let [rid, race] of _.entries(races)) {
    race = _.sortBy(races, i=>parseFloat(i[8]));
    let hids = _.map(race, 6);
    let top3 = hids.slice(0, 3);
    console.log(rid, race.length, top3);
    await Promise.all(top3.map((h) => calc_horse_points(h)));
  }
};
const main = () => {};
const test = async () => {
  let ed = moment().subtract(2, "minutes").toISOString();
  let st = moment(new Date(nano(ed) - dur)).toISOString();
  await run_dur([st, ed]);
};
const tourneyr01 = { test };
module.exports = tourneyr01;
