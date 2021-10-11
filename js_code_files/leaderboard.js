const axios = require("axios");
const _ = require("lodash");
const { init, zed_ch, zed_db } = require("./index-run");
const { read_from_path, write_to_path } = require("./utils");
const {
  get_fee_cat_on,
  download_eth_prices,
  auto_eth_cron,
} = require("./base");
const appRootPath = require("app-root-path");
const { config } = require("dotenv");
const { fetch_a } = require("./fetch_axios");
const { generate_rating_blood } = require("./blood_rating_blood-2");

const day_diff = 1000 * 60 * 60 * 24 * 1;

const get_leaderboard_races = async () => {
  console.log("downloading races");
  let d = Date.now();
  let st = new Date(d - 45 * day_diff).toISOString();
  let races_all = await zed_ch.db
    .collection("zed")
    .find({ 2: { $gt: st } })
    .toArray();
  console.log(races_all.length, "docs downloaded");
  return races_all;
};

const download_leaderboard_races = async () => {
  console.log("downloading races");

  let nano = new Date(new Date().toISOString()?.slice(0, 10));
  let nano_st = nano - 45 * day_diff;
  let nano_ed = nano;

  nano = nano_st;
  while (nano < nano_ed) {
    let st = new Date(nano).toISOString();
    let ed = new Date(nano + day_diff - 1).toISOString();
    let races_today = await zed_ch.db
      .collection("zed")
      .find({ 2: { $gt: st, $lt: ed } })
      .toArray();
    console.log(st.slice(0, 10), ": got", races_today.length);
    let file_path = `${appRootPath}/data/races_bkp/${st}.json`;
    write_to_path({ file_path, data: races_today });
    nano += day_diff;
  }
  return null;
};

const generate_leaderboard = async (races_all) => {
  if (_.isEmpty(races_all)) return;
  races_all = races;
  races_all = _.groupBy(races_all, "6");
  races_all = struct_race_row_data(races_all);

  let hids = _.map(races, "hid");
  let tc_ob = await zed_db.db
    .collection("horse_details")
    .find({ hid: { $in: hids } }, { projection: { _id: 0, tc: 1, hid: 1 } })
    .toArray();
  tc_ob = _.chain(tc_ob)
    .map((e) => [e.hid, e.tc])
    .fromPairs()
    .value();

  let ob = {};
  for (let [hid, races] of _.entries(races_all)) {
    hid = parseInt(hid);
    ob[hid] = await generate_rating_blood({ hid, races, tc: tc_ob[hid] });
  }
};

const runner = async () => {
  await init();
  await download_leaderboard_races();
  console.log("done");
};
runner();

module.exports = {};
