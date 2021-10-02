const _ = require("lodash");
const { zed_db, init } = require("./index-run");
const { fetch_r, delay, read_from_path } = require("./utils");
const axios = require("axios");
const {
  fetch_a,
  fetch_fatigue,
  fetch_horse_zed_api,
} = require("./fetch_axios");
const cron = require("node-cron");
const cron_parser = require("cron-parser");
const appRootPath = require("app-root-path");

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
  console.log("done rating", hid, doc.rating);
};

const upload_horse_fatigue = async (hid) => {
  let doc = await fetch_fatigue(hid);
  if (doc == null) return console.log("err fatigue", hid);
  let id = `hid-doc-${hid}`;
  doc = { id, ...doc, db_date2: new Date().toISOString() };
  await zed_db.db
    .collection("zed_api_cache")
    .updateOne({ id }, { $set: doc }, { upsert: true });
  console.log("done fatigue", hid, doc.current_fatigue);
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

const test_api = (z) => `https://jsonplaceholder.typicode.com/todos/${z}`;

const studs_api = (z) =>
  `https://api.zed.run/api/v1/stud/horses?offset=0&gen[]=${z}&gen[]=${z}`;
const studs_api_cacher = async () => {
  // let apis = [1, 2, 3].map((z) => test_api(z));
  let apis = [1].map((z) => studs_api(z));
  let prev_datas = await Promise.all(
    apis.map((api) => zed_db.db.collection("zed_api_cache").find({ id: api }))
  );
  let datas = await Promise.all(apis.map((api) => fetch_a(api)));
  let db_date = new Date().toISOString();

  for (let [i, data] of _.entries(prev_datas)) {
    if (_.isEmpty(data)) {
      console.log("ERROR prev_data", apis[i]);
      continue;
    }
    data = struct_studs_api_data(data);
    let doc = { id: apis[i], data, db_date };
    write_to_path({
      file_path: `${appRootPath}/data/studs/prev_data-${apis[i]}`,
      data: doc,
    });
    console.log("SUCCESS prev_data", apis[i]);
  }
  for (let [i, data] of _.entries(datas)) {
    if (_.isEmpty(data)) {
      console.log("ERROR data", apis[i]);
      continue;
    }
    data = struct_studs_api_data(data);
    let doc = { id: apis[i], data, db_date };
    write_to_path({
      file_path: `${appRootPath}/data/studs/data-${apis[i]}`,
      data: doc,
    });
    console.log("SUCCESS data", apis[i]);
  }

  // let bulk = [];
  // for (let [i, data] of _.entries(datas)) {
  //   if (_.isEmpty(data)) {
  //     console.log("ERROR", apis[i]);
  //     continue;
  //   }
  //   data = struct_studs_api_data(data);
  //   let doc = { id: apis[i], data, db_date };
  //   console.log("SUCCESS", apis[i]);
  //   bulk.push({
  //     updateOne: {
  //       filter: { id: apis[i] },
  //       update: { $set: doc },
  //       upsert: true,
  //     },
  //   });
  // }
  // if (_.isEmpty(bulk)) return;
  // await zed_db.db.collection("zed_api_cache").bulkWrite(bulk);
  // console.log("# studs_api_cacher", bulk.length, "written");
};
const struct_studs_api_data = (data) => {
  data = data.map((e) => {
    let { bloodline, breed_type, genotype, horse_id: hid, mating_price } = e;
    let name = e.hash_info.name;
    mating_price = parseFloat(mating_price) / 1000000000000000000;
    return {
      hid,
      bloodline,
      breed_type,
      genotype,
      name,
      mating_price,
    };
  });
  return data;
};

const zed_api_cache_runner = async () => {
  await init();
  live_cron();
  horse_update_cron();
};

const studs_api_cache_runner = async () => {
  await init();
  await delay(3000)
  console.log("\n## studs_api_cache_runner started");
  let cron_str = "*/6 * * * * *";
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("Next run:", c_itvl.next().toISOString(), "\n");
  cron.schedule(cron_str, () => studs_api_cacher(), cron_conf);
};

// zed_api_cache_runner()z;

const runner = async () => {
  await init();
  // await studs_api_cacher();
  // let file_path = `${appRootPath}/data/test_studs_data.json`;
  // let data = read_from_path({ file_path });
  // data = struct_studs_api_data(data);
  // console.table(data);
  // console.log("done");

  let id = `https://api.zed.run/api/v1/stud/horses?offset=0&gen[]=1&gen[]=1`;
  let doc = await zed_db.db.collection("zed_api_cache").findOne({ id });
  console.log(doc);
};
// runner();

module.exports = {
  zed_api_cache_runner,
  studs_api_cache_runner,
};
