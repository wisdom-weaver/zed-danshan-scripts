const _ = require("lodash");
const { zed_db, init } = require("./index-run");
const { fetch_r, delay } = require("./utils");
const axios = require("axios");
const {
  fetch_a,
  fetch_fatigue,
  fetch_horse_zed_api,
} = require("./fetch_axios");
const cron = require("node-cron");
const cron_parser = require("cron-parser");

const cron_conf = { scheduled: true };

const cache_url = async (api_url) => {
  let doc = (await fetch_a(api_url)) || null;
  console.log(new Date().toISOString(), "caching", api_url);
  // console.log(doc);
  if (doc == null) {
    console.log("err");
    return;
  } else {
    console.log("got data");
    doc = {
      id: api_url,
      data: doc,
    };
    await zed_db.db
      .collection("zed_api_cache")
      .updateOne({ id: api_url }, { $set: doc }, { upsert: true });
  }
};

const live_base = "https://racing-api.zed.run/api/v1/races?status=open";
const api_urls = (() => {
  let live_urls = [0, 1, 2, 3, 4, 5].map((a) => `${live_base}&class=${a}`);
  return [...live_urls];
})();
const fatigue_urls = () => {
  // https://api.zed.run/api/v1/horses/fatigue/3312
};

const live_upload = async () => {
  console.log(new Date().toISOString());
  for (let api_url of api_urls) {
    await cache_url(api_url);
    await delay(3000);
  }
};

const live_cron = () => {
  console.log("\n## live_cron started");
  let cron_str = "*/20 * * * * *";
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("Next run:", c_itvl.next().toISOString(), "\n");
  cron.schedule(cron_str, () => live_upload(), cron_conf);
};

const upload_horse_dets = async (hid) => {
  let doc = await fetch_horse_zed_api(hid);
  if (doc == null) return console.log("err dets", hid);
  let id = `hid-doc-${hid}`;
  doc = { id, ...doc, db_date: new Date().toISOString() };
  console.log(hid, doc?.current_fatigue);
  await zed_db.db
    .collection("zed_api_cache")
    .updateOne({ id }, { $set: doc }, { upsert: true });
  console.log("done dets", hid);
};

const upload_horse_fatigue = async (hid) => {
  let doc = await fetch_fatigue(hid);
  if (doc == null) return console.log("err fatigue", hid);
  let id = `hid-doc-${hid}`;
  doc = { id, ...doc, db_date2: new Date().toISOString() };
  console.log(hid, doc?.current_fatigue);
  await zed_db.db
    .collection("zed_api_cache")
    .updateOne({ id }, { $set: doc }, { upsert: true });
  console.log("done fatigue", hid);
};

const horse_update_runner = async () => {
  let doc = await zed_db.db
    .collection("zed_api_cache")
    .findOne({ id: "live_my_horses" });
  let { hids = [] } = doc;
  console.log("live_my_horses len:", hids.length);
  for (let hid of hids) {
    await upload_horse_dets(hid);
    await delay(5 * 1000);
    await upload_horse_fatigue(hid);
    await delay(5 * 1000);
  }
};

const horse_update_cron = async () => {
  let i = 0;
  while (true) {
    console.log("started horse bunch cycle", i);
    await horse_update_runner();
    console.log("ended horse bunch cycle", i);
    i++;
  }
};

const zed_api_cache_runner = async () => {
  await init();
  live_cron();
  horse_update_cron();
};
// zed_api_cache_runner();

module.exports = {
  zed_api_cache_runner,
};
