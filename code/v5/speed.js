const _ = require("lodash");
const { init, zed_db, zed_ch } = require("../connection/mongo_connect");
const {
  calc_avg,
  pad,
  calc_median,
  fetch_r,
  struct_race_row_data,
  read_from_path,
  write_to_path,
  dec,
  getv,
} = require("../utils/utils");
const app_root = require("app-root-path");
const {
  get_races_of_hid,
  get_ed_horse,
  get_range_hids,
  valid_b5,
  get_parents,
} = require("../utils/cyclic_dependency");
const bulk = require("../utils/bulk");
const cyclic_depedency = require("../utils/cyclic_dependency");
const utils = require("../utils/utils");
const moment = require("moment");

let mx = 11000;
let st = 1;
let ed = mx;
let cs = 25;

let tot_runs = 1;
const name = "speed";
const coll = "speed";
let test_mode = 0;

const dist_factor = {
  1000: 1.0,
  1200: 1.03141812,
  1400: 1.03585102,
  1600: 1.03832922,
  1800: 1.03984722,
  2000: 1.04114768,
  2200: 1.04225978,
  2400: 1.04259176,
  2600: 1.04369422,
};

const calc_speed_from_races = (races) => {
  if (_.isEmpty(races)) return null;
  let mx = _.maxBy(races, (i) => {
    return getv(i, "finishtime");
  });
  let { distance, finishtime } = mx;
  let speed = dist_factor[distance] * finishtime;
  // console.log("max_speed", { distance, finishtime, speed });
  return speed;
};

const calc = async ({ hid, races }) => {
  try {
    let speed = calc_speed_from_races(races);
    let ob = { hid, speed };
    console.log(ob);
    return ob;
  } catch (err) {
    console.log("err on horse speed", hid);
    console.log(err);
    return null;
  }
};

const generate = async (hid) => {
  try {
    hid = parseInt(hid);
    let st = moment().add(-90, "days").toISOString();
    let ed = moment().add(0, "days").toISOString();
    let races = await zed_ch.db
      .collection("zed")
      .find(
        { 2: { $gte: st, $lte: ed }, 6: hid },
        { projection: { 1: 1, 7: 1 } }
      )
      .toArray();
    // console.table(races);
    races = cyclic_depedency.struct_race_row_data(races);
    let speed = calc_speed_from_races(races);
    let ob = { hid, speed };
    return ob;
  } catch (err) {
    console.log("err in speed", err);
  }
};

const all = async () => {
  console.log(name, "all");
  await bulk.run_bulk_all(name, generate, coll, cs, test_mode);
};
const only = async (hids) => {
  console.log(name, "only");
  if (hids[0] == "b5") {
    hids = await zed_db.db
      .collection("horse_details")
      .find({ tx_date: { $gte: v5_conf.st_date } }, { projection: { hid: 1 } })
      .toArray();
    hids = _.map(hids, "hid");
    console.log("b5", hids);
  }
  await bulk.run_bulk_only(name, generate, coll, hids, cs, test_mode);
};
const range = async ([st, ed]) => {
  console.log(name, "range", st, ed);
  if (!ed) ed = await get_ed_horse();
  await bulk.run_bulk_range(name, generate, coll, st, ed, cs, test_mode);
};

const speed = {
  generate,
  calc,
  // test,
  all,
  only,
  range,
};
module.exports = speed;
