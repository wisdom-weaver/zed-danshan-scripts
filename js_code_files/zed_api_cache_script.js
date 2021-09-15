const _ = require("lodash");
const { zed_db, init } = require("./index-run");
const { fetch_r, delay } = require("./utils");
const axios = require("axios");
const { fetch_a, fetch_fatigue } = require("./fetch_axios");
const cron = require("node-cron");
const cron_parser = require("cron-parser");

const cron_conf = { scheduled: true };

const cache_url = async (api_url) => {
  let doc = (await fetch_a(api_url)) || null;
  console.log("caching", api_url);
  if (doc == null) {
    console.log("err");
    return;
  }
  {
    console.log("got data");
    doc = {
      id: api_url,
      data: doc,
    };
  }
  await zed_db.db
    .collection("zed_api_cache")
    .updateOne({ id: api_url }, { $set: doc }, { upsert: true });
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
    await delay(4000);
  }
};

const live_cron = () => {
  console.log("\n## live_cron started");
  let cron_str = "* * * * *";
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("Next run:", c_itvl.next().toISOString(), "\n");
  cron.schedule(cron_str, () => live_upload(), cron_conf);
};

const zed_api_cache_runner = async () => {
  await init();
  live_cron();
};
// zed_api_cache_runner();

module.exports = {
  zed_api_cache_runner,
};
