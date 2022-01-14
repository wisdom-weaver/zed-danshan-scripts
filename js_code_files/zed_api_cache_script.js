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
const zedf = require("./zedf");

const cron_conf = { scheduled: true };

const cache_url = async (api_url) => {
  // let doc = (await fetch_a(api_url)) || null;
  let doc = await zedf.get(api_url);
  // console.log(new Date().toISOString(), "caching", api_url);
  // console.log(doc);
  let ext_url = api_url.slice(api_url.lastIndexOf("/"));
  if (doc == null) {
    console.log("err", _.keys(doc).length, ext_url);
    return;
  } else {
    console.log("got", _.keys(doc).length, ext_url);
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
    await delay(1000);
  }
  console.log("\n");
};

const live_cron = () => {
  console.log("\n## live_cron started");
  let cron_str = "*/8 * * * * *";
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("Next run:", c_itvl.next().toISOString(), "\n");
  cron.schedule(cron_str, () => live_upload(), cron_conf);
};

const upload_horse_dets = async (hid) => {
  // let doc = await fetch_horse_zed_api(hid);
  let doc = await zedf.horse(hid);
  if (doc == null) return console.log("err dets", hid);
  let id = `hid-doc-${hid}`;
  doc = { id, ...doc, db_date: new Date().toISOString() };
  await zed_db.db
    .collection("zed_api_cache")
    .updateOne({ id }, { $set: doc }, { upsert: true });
  console.log("done rating", hid, doc.rating);
};

const upload_horse_fatigue = async (hid) => {
  // let doc = await fetch_fatigue(hid);
  let doc = await zedf.fatigue(hid);
  if (doc == null) return console.log("err fatigue", hid);
  let id = `hid-doc-${hid}`;
  doc = { id, ...doc, db_date2: new Date().toISOString() };
  await zed_db.db
    .collection("zed_api_cache")
    .updateOne({ id }, { $set: doc }, { upsert: true });
  console.log("done fatigue", hid, doc.current_fatigue);
};

const horse_dets_update_runner = async () => {
  let doc = await zed_db.db
    .collection("zed_api_cache")
    .findOne({ id: "live_my_horses" });
  let { hids = [] } = doc;
  console.log("live_my_horses len:", hids.length);
  for (let hid of hids) {
    await upload_horse_dets(hid);
    await delay(2 * 1000);
  }
};
const horse_fatigue_update_runner = async () => {
  let doc = await zed_db.db
    .collection("zed_api_cache")
    .findOne({ id: "live_my_horses" });
  let { hids = [] } = doc;
  console.log("live_my_horses len:", hids.length);
  for (let hid of hids) {
    await upload_horse_fatigue(hid);
    await delay(30 * 1000);
  }
};

const horse_rating_update_cron = async () => {
  let i = 0;
  while (true) {
    console.log("started horse rating bunch cycle", i);
    await horse_dets_update_runner();
    console.log("ended horse rating bunch cycle", i);
    i++;
  }
};
const horse_fatigue_update_cron = async () => {
  let i = 0;
  while (true) {
    console.log("started horse fatigue bunch cycle", i);
    await horse_fatigue_update_runner();
    console.log("ended horse fatigue bunch cycle", i);
    i++;
  }
};

const test_api = (z) => `https://jsonplaceholder.typicode.com/todos/${z}`;

const studs_api = (z, o = 0) =>
  `https://api.zed.run/api/v1/stud/horses?offset=${o}&gen[]=${z}&gen[]=${z}&breed_type=${"genesis"}`;

let process_prev_curr_studs = (prev, curr) => {
  console.log("all", _.map(curr.new, "hid"));
  if (_.isEmpty(prev)) prev = { new: [], old: [] };
  let new_horses = [];
  let old_horses = [];
  let prevsno = [...prev.new, ...prev.old];
  for (let h of curr.new) {
    let { hid, date } = h;
    if (_.map(prevsno, "hid").includes(hid) == false) new_horses.push(h);

    if (_.map(prev.new, "hid").includes(hid)) {
      let { date: dpn } = _.find(prev.new, { hid });
      if (new Date(date).getTime() - new Date(dpn).getTime() < 3 * 60 * 1000)
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
  let doc = { ...ar };
  let id = "zed-studs-sniper";
  doc.id = id;
  await zed_db.db
    .collection("zed_api_cache")
    .updateOne({ id }, { $set: doc }, { upsert: true });
};
const studs_api_cacher = async (z) => {
  try {
    console.log("START studs_api_cacher");
    let id = "zed-studs-sniper";
    let z_apis = [
      [1, 0],
      [1, 10],
      [1, 20],
      [2, 0],
      [2, 10],
      [2, 20],
      [3, 0],
      [3, 10],
      [3, 20],
    ].map(([z, o]) => studs_api(z, o));
    let api_doc = await Promise.all(z_apis.map((z_api) => zedf.get(z_api)));
    if (api_doc.includes(null)) {
      console.log("SKIPPED either api gave null");
      return;
    }
    console.log("fetched", api_doc.length);

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
    console.log(err);
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
  // horse_rating_update_cron();
  // horse_fatigue_update_cron();
};
const live_api_cache_runner = async () => {
  await init();
  live_cron();
  // horse_rating_update_cron();
  // horse_fatigue_update_cron();
};

const studs_clear = async () => {
  let id = "zed-studs-sniper";
  let doc = {};
  doc.id = id;
  doc.new = [];
  doc.old = [];
  await zed_db.db
    .collection("zed_api_cache")
    .updateOne({ id }, { $set: doc }, { upsert: true });
};
const studs_print = async () => {
  let doc = await zed_db.db
    .collection("zed_api_cache")
    .findOne({ id: "zed-studs-sniper" });
  console.log(doc);
};
const studs_api_cache_runner = async () => {
  await init();
  await delay(3000);
  // studs_api_cacher();
  // studs_api_cacher_test();
  // studs_clear();
  // studs_print();
  // console.log("done");

  console.log("\n## studs_api_cache_runner started");
  let cron_str = "*/10 * * * * *";
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("Next run:", c_itvl.next().toISOString(), "\n");
  cron.schedule(cron_str, () => studs_api_cacher(), cron_conf);
};

// zed_api_cache_runner()z;

const runner = async () => {
  await init();
  console.log(await zedf.fatigue(3312));
};
// runner();

const horse_dets_test = async () => {
  let hid = 3312;
  console.log("hid: ", hid);
  let doc = await fetch_horse_zed_api(hid);
  console.log("doc->", doc);
  console.log("done");
};

module.exports = {
  zed_api_cache_runner,
  live_api_cache_runner,
  studs_api_cache_runner,
  horse_dets_test,
};
