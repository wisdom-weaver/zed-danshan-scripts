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
  let mx = _.maxBy(races, (i) => {
    return getv(i, "finishtime");
  });
  let { distance, finishtime } = mx;
  let speed = dist_factor[distance] * finishtime;
  console.log("max_speed", { distance, finishtime, speed });
};

const calc = async ({ hid, races }) => {
  try {
    let speed = calc_speed_from_races(races);
    return { hid, speed };
  } catch (err) {
    console.log("err on horse speed", hid);
    console.log(err);
    return null;
  }
};
const generate = async (hid) => {
  let st = moment().add(-90, "days").toISOString();
  let ed = moment().add(0, "days").toISOString();
  console.log(st, ed);
  let races = await zed_ch.db
    .collection("zed")
    .find(
      {
        2: { $gte: st, $lte: ed },
      },
      {
        projection: { 1: 1, 7: 1 },
      }
    )
    .toArray();
  races = cyclic_depedency.struct_race_row_data(races);
  // console.table(races);
  let ob = await calc({ hid, races });
  return ob;
};
const test = async (hids) => {
  test_mode = 1;
  for (let hid of hids) {
    let ob = await generate(hid);
    console.log(hid, ob);
  }
};

const all = async () => bulk.run_bulk_all(name, generate, coll, cs, test_mode);
const only = async (hids) =>
  bulk.run_bulk_only(name, generate, coll, hids, cs, test_mode);
const range = async ([st, ed]) => {
  st = parseInt(st);
  ed = parseInt(ed);
  bulk.run_bulk_range(name, generate, coll, st, ed, cs, test_mode);
};

const fixer = async () => {
  let all_hids = await cyclic_depedency.get_all_hids();
  all_hids = all_hids.slice(157001);
  for (let chunk of _.chunk(all_hids, 2000)) {
    let [a, b] = [chunk[0], chunk[chunk.length - 1]];
    console.log(a, "->", b);
    await Promise.all(
      chunk.map((hid) => {
        return zed_db.db
          .collection(coll)
          .updateOne({ hid }, { $set: null_br_doc(hid) }, { upsert: true });
      })
    );
  }
  console.log("ENDED fixer");
};

const speed = {
  generate,
  calc,
  test,
  all,
  only,
  range,
  fixer,
};
module.exports = speed;
