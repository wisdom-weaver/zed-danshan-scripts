const _ = require("lodash");
const prompt = require("prompt-sync")();
const fetch = require("node-fetch");
const mongoose = require("mongoose");
const { init, zed_db, zed_ch, run_func } = require("./index-run");
const { fetch_r, delay } = require("./utils");
const app_root = require("app-root-path");
const { ObjectId } = require("mongodb");
const readline = require("readline");
const { download_eth_prices, get_fee_cat_on } = require("./base");

let from_date;
let to_date = new Date(Date.now()).toISOString().slice(0, 10);
let chunk_size = 10;
let chunk_delay = 100;
let cutoff_date = "2021-08-25T19:00:00.000Z";
let default_from_date = "2021-08-26";

//global
let avg_ob = {};
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
];
const key_no = (key) => {
  let r = _.find(key_mapping_bs_zed, { 1: key });
  return r ? r[0] : null;
};

const get_flames = async (rid) => {
  if (!rid) return null;
  let api = `https://rpi.zed.run/?race_id=${rid}`;
  let doc = await fetch_r(api);
  return doc?.rpi || null;
};

const set_flame_for_horse = async ({ rid, hid, flame }) => {
  try {
    let doc = await zed_ch.db.collection("zed").findOne({ 4: rid, 6: hid });
    if (doc == null) {
      // console.log("missing horse", hid, " document in", rid);
      return;
    }
    let _id = ObjectId(doc?._id);
    let date = doc["2"];
    let update_ob = { 13: flame };
    if (date >= cutoff_date) {
      let d = doc["1"];
      let fee = doc["3"];
      let c = doc["5"];
      let p = doc["8"];
      let fee_cat = get_fee_cat_on({ date, fee });
      let key = [c, fee_cat, d, p, flame].join("_");
      let odds = avg_ob[key] || 0;
      update_ob["11"] = odds;
    }
    await zed_ch.db.collection("zed").updateOne({ _id }, { $set: update_ob });
  } catch (err) {
    console.log("err on set_flame_for_horse", rid, hid);
  }
};

const add_flames_to_race = async (rid) => {
  try {
    let flames = await get_flames(rid);
    if (_.isEmpty(flames)) return console.log("couldn't load flames for", rid);
    // console.log(flames);
    let hids = _.keys(flames).map(parseFloat);
    await Promise.all(
      hids.map((hid) => set_flame_for_horse({ hid, rid, flame: flames[hid] }))
    );
  } catch (err) {
    console.log("err add_flames_to_race ", rid, err.message);
  }
};

const print_race = async (rid) => {
  let doc = await zed_ch.db.collection("zed").find({ 4: rid }).toArray();
  console.log("race=>", rid);
  console.table(doc);
};

const find_raceids_on_date = async (date) => {
  let st_date = new Date(date).toISOString();
  let day_diff = 24 * 60 * 60 * 1000 - 1;
  let ed_date = new Date(new Date(st_date).getTime() + day_diff).toISOString();
  let docs = await zed_ch.db
    .collection("zed")
    .find(
      { 2: { $gt: st_date, $lt: ed_date } },
      { projection: { 4: 1, _id: 0 } }
    )
    .toArray();
  let rids = _.isEmpty(docs) ? [] : _.map(docs, "4");
  return _.uniq(rids);
};
const progress_bar = (a, b) => {
  let len = 50;
  let per = parseFloat((a / b) * 100).toFixed(2);
  let eqs = new Array(Math.ceil((len * a) / b)).fill("=").join("");
  let dts = new Array(Math.ceil(len * (1 - a / b))).fill(".").join("");
  return `[${eqs}>${dts}] ${per}%| ${a}/${b}`;
};
const add_flames_to_race_on_date = async (date) => {
  if (!date) return;
  let date_str = date.slice(0, 10);
  process.stdout.write(`\n${date_str} : getting race_ids  \r`);
  let rids = await find_raceids_on_date(date);
  if (_.isEmpty(rids)) {
    process.stdout.write(`${date_str} : no races found   `);
    return;
  }
  process.stdout.write(`${date_str} : ${rids.length} races found\r`);
  let i = 0;
  let n = rids.length;
  for (let chunk of _.chunk(rids, chunk_size)) {
    process.stdout.write(`${date_str} : ${progress_bar(i, n)} \r`);
    await Promise.all(chunk.map(add_flames_to_race));
    i += chunk.length;
    await delay(chunk_delay);
  }
  process.stdout.write(`${date_str} : ${progress_bar(n, n)} races\r`);
};

const add_flames_to_race_from_to_date = async (from, to) => {
  console.log("from: ", from);
  console.log("to  : ", to);
  let st_d = new Date(from).getTime();
  let ed_d = new Date(to).getTime();
  let dif = 24 * 60 * 60 * 1000;
  for (let now = st_d; now <= ed_d; now += dif) {
    let today = new Date(now).toISOString();
    try {
      await add_flames_to_race_on_date(today);
      let update_ob = { last_updated: today };
      await zed_db.db
        .collection("odds_avg")
        .updateOne({ id: "odds_avg_ALL" }, { $set: update_ob });
    } catch (err) {
      console.log("\nerr getting races on ", today);
    }
  }
};

const add_flames_on_all_races = async () => {
  await init();
  await download_eth_prices();
  let doc = await zed_db.db
    .collection("odds_avg")
    .findOne({ id: "odds_avg_ALL" });
  avg_ob = doc?.avg_ob;
  if (_.isEmpty(avg_ob)) return console.log("Failed to load avgs");
  console.log("avgs loaded", _.keys(avg_ob).length);
  console.log("last updated date:", doc?.last_updated);

  from_date = doc?.last_updated || default_from_date;
  from_date = new Date(from_date).toISOString().slice(0, 10);

  await add_flames_to_race_from_to_date(from_date, to_date);
  console.log("\n=========\nCOMPLETED");
  await zed_db.close();
  await zed_ch.close();
  return;
  process.exit();
};
// add_flames_on_all_races();

module.exports = {
  add_flames_on_all_races,
};
