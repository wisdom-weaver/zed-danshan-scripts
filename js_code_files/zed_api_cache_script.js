const _ = require("lodash");
const { zed_db, init } = require("./index-run");
const { fetch_r, delay, read_from_path, write_to_path } = require("./utils");
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

let process_prev_curr_studs = (prev, curr) => {
  if (_.isEmpty(prev)) prev = { new: [], old: [] };
  let new_horses = [];
  let old_horses = [];
  let prevsno = [...prev.new, ...prev.old];
  for (let h of curr.new) {
    let { hid, date } = h;
    if (_.map(prevsno, "hid").includes(hid) == false) new_horses.push(h);

    if (_.map(prev.new, "hid").includes(hid)) {
      let { date: dpn } = _.find(prev.new, { hid });
      if (new Date(date).getTime() - new Date(dpn).getTime() < 10 * 60000)
        new_horses.push({ ...h, date: dpn });
      else old_horses.push({ ...h, date: dpn });
    }

    if (_.map(prev.old, "hid").includes(hid)) {
      let { date: dpo } = _.find(prev.old, { hid });
      if (new Date(date).getTime() - new Date(dpo).getTime() < 24 * 60 * 60000)
        old_horses.push({ ...h, date: dpo });
    }
  }
  console.log("new:", _.map(new_horses, "hid"));
  console.log("old:", _.map(old_horses, "hid"));
  return { new: new_horses, old: old_horses };
};
const studs_api_cacher_test = async () => {
  let a = read_from_path({
    file_path: `${appRootPath}/data/studs/data_z1_01.json`,
  });
  let b = read_from_path({
    file_path: `${appRootPath}/data/studs/data_z1_02.json`,
  });
  let c = read_from_path({
    file_path: `${appRootPath}/data/studs/data_z1_03.json`,
  });
  let d = Date.now();

  a.data = a.data.map((e) => ({
    ...e,
    date: new Date(d - 10 * 60000).toISOString(),
  }));
  b.data = b.data.map((e) => ({
    ...e,
    date: new Date(d - 20000).toISOString(),
  }));
  c.data = c.data.map((e) => ({ ...e, date: new Date(d).toISOString() }));

  a = { new: a.data, old: [] };
  b = { new: b.data, old: [] };
  c = { new: c.data, old: [] };

  let ar = process_prev_curr_studs({ new: [], old: [] }, a);
  console.log("------------");
  ar = process_prev_curr_studs(ar, b);
  await delay(2000);
  console.log("------------");
  ar = process_prev_curr_studs(ar, c);
};
const studs_api_cacher = async (z) => {
  try {
    console.log("START studs_api_cacher");
    let id = "zed-studs-sniper";
    let z_apis = [1, 2, 3].map((z) => studs_api(z));
    let api_doc = await Promise.all(z_apis.map((z_api) => studs_api(z_api)));
    api_doc = _.chain(api_doc).compact().flatten().value() || [];
    api_doc = struct_studs_api_data(api_doc);
    if (_.isEmpty(api_doc)) return;
    api_doc = { new: api_doc, old: [] };
    let mdb_doc = await zed_db.db.collection("zed_api_cache").findOne({ id });
    if (_.isEmpty(mdb_doc)) mdb_doc = { new: [], old: [] };
    let doc = process_prev_curr_studs(mdb_doc, api_doc) || { new: [], old: [] };
    doc.id = id;
    await zed_db.db
      .collection("zed_api_cache")
      .updateOne({ id }, { $set: doc }, { upsert: true });
    console.log("DONE  studs_api_cacher");
  } catch (err) {
    console.log("ERROR studs_api_cacher");
  }
};
const struct_studs_api_data = (data) => {
  let date = new Date().toISOString();
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
      date,
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
  await delay(3000);
  // studs_api_cacher();
  console.log("\n## studs_api_cache_runner started");
  let cron_str = "*/6 * * * * *";
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("Next run:", c_itvl.next().toISOString(), "\n");
  cron.schedule(cron_str, () => studs_api_cacher(), cron_conf);
  console.log("done")
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
