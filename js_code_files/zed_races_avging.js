const _ = require("lodash");
const fetch = require("node-fetch");
const mongoose = require("mongoose");
const { init, zed_db, zed_ch, run_func } = require("./index-run");
const { fetch_r, delay, write_to_path, read_from_path } = require("./utils");
const app_root = require("app-root-path");
const { ObjectId } = require("mongodb");
const readline = require("readline");
const { struct_race_row_data } = require("./index-odds-generator");
const { download_eth_prices, get_fee_cat_on } = require("./base");

let from_date = "2020-12-31";
let to_date = "2021-08-25";
let chunk_size = 10;
let chunk_delay = 100;

//global
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

const key_mapping_bs_zed = [
  ["_id", "_id"],
  ["1", "distance"],
  ["2", "date"],
  ["3", "entryfee"],
  ["4", "raceid"],
  ["5", "thisclass"],
  ["6", "hid"],
  ["7", "finishtime"],
  ["8", "place"],
  ["9", "name"],
  ["10", "gate"],
  ["11", "odds"],
  ["12", "unknown"],
  ["13", "flame"],
  ["14", "fee_cat"],
  ["15", "adjfinishtime"],
];
const key_no = (key) => {
  let r = _.find(key_mapping_bs_zed, { 1: key });
  return r ? r[0] : null;
};
let avg_ob = {};
const avg_ob_out_path = `${app_root}/races_avg/races-avg.json`;

const classes = [0, 1, 2, 3, 4, 5];
const dists = [1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600];
const fee_cats = ["A", "B", "C", "F"];
const places = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
const day_diff = 1000 * 60 * 60 * 24;

const get_today_st_ed = (date) => {
  date = new Date(date).getTime();
  let st = new Date(date).toISOString();
  let ed = new Date(date + day_diff - 1).toISOString();
  return { date_st: st, date_ed: ed };
};

const get_keys = () => {
  let keys = [];
  for (let c of classes)
    for (let f of fee_cats)
      for (let d of dists)
        for (let p of places)
          for (let F of [0, 1]) {
            keys.push(`${c}_${f}_${d}_${p}_${F}`);
          }
  return keys;
};
const keys = get_keys();
const default_avg_ob = () => {
  let ob = {};
  for (let k of keys) {
    ob[k] = 0;
    ob[k + "_n"] = 0;
  }
  return ob;
};

const struct_race_row_data_with_fees = (docs) => {
  docs = struct_race_row_data(docs);
  docs = docs.map((e) => {
    let { entryfee: fee, date } = e;
    let fee_cat = get_fee_cat_on({ fee, date });
    return { ...e, fee_cat };
  });
  return docs;
};

const make_key_from_doc = (doc) => {
  let { distance: d, thisclass: c, place: p, flame: F, fee_cat: f } = doc;
  let key = [c, f, d, p, F].join("_");
  return key;
};

const get_races_for_each_date = async (date) => {
  let { date_st, date_ed } = get_today_st_ed(date);
  let docs = await zed_ch.db
    .collection("zed")
    .find({ 2: { $gt: date_st, $lt: date_ed } })
    .toArray();
  docs = struct_race_row_data_with_fees(docs);
  docs = docs.map((doc) => ({ ...doc, key: make_key_from_doc(doc) }));
  return docs;
};

const update_to_avg_json = ({ now, avg_ob }) => {
  write_to_path({
    file_path: avg_ob_out_path,
    data: { last_date: new Date(now).toISOString(), avg_ob },
  });
};

const update_avg_ob_for_races = ({ races }) => {
  for (let key of keys) {
    let fr = _.filter(races, { key });
    let n = fr.length;
    if (n == 0) continue;

    let avg = _.mean(_.map(fr, "odds"));
    let key_n = key + "_n";

    let prev_n_tot = avg_ob[key_n] || 0;
    let prev_odds_tot = avg_ob[key] * prev_n_tot;
    let new_n_tot = avg_ob[key_n] + n;
    let new_odds_tot = (prev_odds_tot + avg * n) / new_n_tot;

    avg_ob[key] = new_odds_tot;
    avg_ob[key_n] = new_n_tot;
  }
};

const get_avg_odds_for_all_cfdp_flames = async () => {
  await init();
  await download_eth_prices();
  let avg_json = read_from_path({ file_path: avg_ob_out_path }) || null;
  console.log(`Need Data \nfrom:${from_date}\nto  :${to_date} `);

  if (_.isEmpty(avg_json)) {
    avg_ob = default_avg_ob();
  } else {
    console.log("found old data");
    avg_ob = avg_json.avg_ob;
    from_date = avg_json.last_date;
  }
  console.log("#starting from ", from_date);

  let now = new Date(from_date).getTime();
  let end_limit = new Date(to_date).getTime();
  for (; now <= end_limit; now += day_diff) {
    let date = new Date(now).toISOString();
    let date_str = date.slice(0, 10);
    process.stdout.write(`\ndate: ${date_str} loading...\r`);
    let races = await get_races_for_each_date(date);
    process.stdout.write(`date: ${date_str} done        `);
    update_avg_ob_for_races({ races });
    update_to_avg_json({ now, avg_ob });
    await delay(1000);
  }
  console.log("\n## ENDED");
};
// get_avg_odds_for_all_cfdp_flames();

const upload_avg_ob_to_mongo = async () => {
  await init();
  let avg_json = read_from_path({ file_path: avg_ob_out_path }) || null;
  let id = "odds_avg_ALL";
  avg_json.id = id;
  await zed_db.db
    .collection("odds_avg")
    .updateOne({ id }, { $set: avg_json }, { upsert: true });
  console.log("done");
};
// upload_avg_ob_to_mongo();
const print_table = () => {
  let avg_json = read_from_path({ file_path: avg_ob_out_path }) || null;
  let id = "odds_avg_ALL";
  avg_ob = avg_json.avg_ob;
  let table = [];
  let c = 0;
  let f = "F";
  let d = 1400;
  let F = 1;

  for (let p = 1; p <= 12; p++) {
    let key = [c, f, d, p, F].join("_");
    let avg = avg_ob[key];
    let no_of_horses_here = avg_ob[key + "_n"];
    table.push({
      key,
      class: c,
      fee_cate: f,
      dist: d,
      place: p,
      Flame: F,
      avg,
      no_of_horses_here,
    });
  }
  console.table(table);
};
// print_table();

module.exports = {
  get_avg_odds_for_all_cfdp_flames,
};
