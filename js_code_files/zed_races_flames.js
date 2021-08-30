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
const {
  generate_odds_for,
  generate_blood_mapping,
  give_ranks_on_rating_blood,
} = require("./index-odds-generator");
const { get_parents_hids, get_kids_and_upload } = require("./horses-kids");

let from_date;
let to_date = new Date(Date.now()).toISOString().slice(0, 10);
let chunk_size = 10;
let chunk_delay = 100;
let cutoff_date = "2021-08-25T19:00:00.000Z";
let default_from_date = "2021-08-26";

//global
let avg_ob = {};
let hids = [];
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
  let rpi = doc?.rpi || {};
  let n_hids = _.keys(rpi);
  hids = [...hids, ...n_hids];
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
    // .limit(20)
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
  return rids || [];
};

const get_hids_of_race = async (rid) => {
  let flames_ob = (await get_flames(rid)) || {};
  return _.keys(flames_ob) || [];
};
const get_hids_in_races = async (rids = []) => {
  let hids = [];
  let i = 0,
    n = rids.length;
  for (let chunk of _.chunk(rids, chunk_size)) {
    process.stdout.write(`${progress_bar(i, n)} \r`);
    let data = await Promise.all(chunk.map((rid) => get_hids_of_race(rid)));
    let chunk_hids = _.flatten(data);
    hids = [...hids, ...chunk_hids];
    i += chunk.length;
  }
  process.stdout.write(`${progress_bar(n, n)} \r`);
  return _.uniq(hids);
};

const add_flames_to_race_from_to_date = async (from, to) => {
  let all_rids = [];
  console.log("from: ", from);
  console.log("to  : ", to);
  let st_d = new Date(from).getTime();
  let ed_d = new Date(to).getTime();
  let dif = 24 * 60 * 60 * 1000;
  for (let now = st_d; now <= ed_d; now += dif) {
    let today = new Date(now).toISOString();
    try {
      let rids = await add_flames_to_race_on_date(today);
      all_rids = [...all_rids, ...rids];
      console.log("getting horses in races:", rids.length);
    } catch (err) {
      console.log("\nerr getting races on ", today);
    }
  }
  return all_rids;
};

const odds_generator_for = async (hids) => {
  let i = 0;
  let n = hids.length;
  console.log("\n##odds_generator_for hids:", n);
  for (let chunk of _.chunk(hids, 25)) {
    process.stdout.write(`odds  : ${progress_bar(i, n)} \r`);
    await Promise.all(chunk.map((hid) => generate_odds_for(hid)));
    i += chunk.length;
  }
  process.stdout.write(`odds  : ${progress_bar(n, n)} \r`);
  console.log("\n##COMPLETED odds_generator_for");
};

const breed_generator_for_hid_parents = async (hid, log = 0) => {
  hids = parseInt(hid);
  if (hid == null) return;
  let { mother = null, father = null } = await get_parents_hids(hid);
  if (log == 1) console.log(hid, mother, father);
  await get_kids_and_upload(mother, log);
  await get_kids_and_upload(father, log);
  await get_kids_and_upload(hid, log);
};

const breed_generator_parents_of_all_horses = async (hids = [], log) => {
  let i = 0;
  let cs = 1;
  let n = hids.length;
  console.log("\n##breed_generator_parents_of_all_horses:", n);
  for (let chunk of _.chunk(hids, cs)) {
    process.stdout.write(`breed : ${progress_bar(i, n)} \r`);
    await Promise.all(
      chunk.map((hid) => breed_generator_for_hid_parents(hid, log))
    );
    i += cs;
    delay(100);
  }
  process.stdout.write(`breed : ${progress_bar(n, n)} \r`);
  console.log("\n##COMPLETED breed_generator_parents_of_all_horses");
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

  // from_date = "2020-12-31";
  // to_date = "2020-12-31";

  await add_flames_to_race_from_to_date(from_date, to_date);
  console.log("\n\n## GOT all races in the duration");
  
  hids = hids.sort();
  hids = _.uniq(hids);
  console.log("horses=>");
  console.log(JSON.stringify(hids));

  await odds_generator_for(hids);

  await breed_generator_parents_of_all_horses(hids,1);

  let update_ob = { last_updated: to_date.slice(0, 10) };
  
  await zed_db.db
    .collection("odds_avg")
    .updateOne({ id: "odds_avg_ALL" }, { $set: update_ob });

  console.log("generate_blood_mapping");
  await generate_blood_mapping();
  console.log("give_ranks_on_rating_blood");
  await give_ranks_on_rating_blood();
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
