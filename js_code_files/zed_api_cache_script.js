const _ = require("lodash");
const { zed_db, init } = require("./index-run");
const { fetch_r } = require("./utils");

const cache_url = async (api_url) => {
  const doc = (await fetch_r(api_url)) || null;
  console.log("caching", api_url);
  if (doc == null) return;
  doc = {
    id: api_url,
    data: doc,
  };
  await zed_db.db
    .collection("zed_api_cache")
    .updateOne({ id: api_url }, { $set: doc }, { upsert: true });
};

const test_runner = async () => {
  let api_url = "https://racing-api.zed.run/api/v1/races?status=open&class=0";
  await cache_url(api_url);
  console.log("done");
};
const zed_api_cache_runner = async () => {
  await init();
  await test_runner();
  console.log("ended");
};

module.exports = {
  zed_api_cache_runner,
};
