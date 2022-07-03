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
  get_hids,
  nano,
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
const { sheet_print_ob } = require("../../sheet_ops/sheets_ops");

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

const calc = async ({ hid, races }) => {
  try {
    // console.log("calc", races.length);
    races = _.sortBy(races, "date");
    races = cyclic_depedency.filter_r1000(races);

    let st = moment().add(-90, "days").toISOString();
    let ed = moment().add(0, "days").toISOString();
    let ar = await zed_ch.db
      .collection("zed")
      .aggregate([
        { $match: { 6: hid, 2: { $gte: st, $lte: ed }, 25: { $ne: null } } },
        { $sort: { 25: -1 } },
        {
          $project: {
            _id: 0,
            hid: "$6",
            speed: "$25",
            adjtime: "$23",
            distance: "$1",
          },
        },
        { $limit: 2 },
      ])
      .toArray();
    if (test_mode) console.table(ar);
    let speed_ob;
    if (ar.length > 1) {
      let sp1 = getv(ar, `${0}.speed`);
      let sp2 = getv(ar, `${1}.speed`);
      if (Math.abs(sp2 - sp1) <= 0.7) speed_ob = getv(ar, 0);
      else speed_ob = getv(ar, 1);
    } else {
      speed_ob = getv(ar, "0");
    }
    if (_.isEmpty(speed_ob)) speed_ob = { hid, distance: null, speed: null };
    if (test_mode) console.log(speed_ob);
    return speed_ob;
  } catch (err) {
    console.log("err on horse speed", hid);
    console.log(err);
    return null;
  }
};

const generate = async (hid) => {
  try {
    // console.log("generate");
    hid = parseInt(hid);
    let ob = await calc({ hid });
    // console.log(ob)
    return ob;
  } catch (err) {
    console.log("err in speed", err);
    console.log(err);
  }
};

const all = async () => {
  console.log(name, "all");
  let [st, ed] = [1, await get_ed_horse()];
  for (let i = st; i <= ed; i += cs) {
    console.log(i, i + cs - 1);
    let hids = get_hids(i, i + cs - 1);
    hids = await filt_valid_hids(hids);
    console.log("valids:", hids.length);
    await bulk.run_bulk_only(name, generate, coll, hids, cs, test_mode);
  }
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
  dist_factor,
};
module.exports = speed;
