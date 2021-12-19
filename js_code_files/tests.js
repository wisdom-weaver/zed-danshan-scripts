const { generate_blood_mapping } = require("./index-odds-generator");
const {
  compare_heads,
  check_h2_better_h1,
  best_in_battlefield,
  had_last_race_in_days,
} = require("./leaderboard-generator");
const { init, zed_db, zed_ch } = require("./index-run");
const _ = require("lodash");
const mongoose = require("mongoose");
const {
  write_to_path,
  read_from_path,
  fetch_r,
  delay,
  struct_race_row_data,
} = require("./utils");
const app_root = require("app-root-path");
const { MongoClient } = require("mongodb");
const fetch = require("node-fetch");
const { download_eth_prices } = require("./base");
const fs = require("fs");
const { ObjectId } = require("mongodb");
const {
  generate_rating_blood,
  generate_rating_blood_from_hid,
} = require("./blood_rating_blood-2");
const {
  update_odds_and_breed_for_race_horses,
} = require("./odds-generator-for-blood2");
const { get_ed_horse } = require("./zed_horse_scrape");

const test_run = async () => {
  await init();
  let st = 1;
  let ed = await get_ed_horse();
  let cs = 500;
  let hids = new Array(ed - st + 1).fill(0).map((ea, idx) => st + idx);
  let fpth = (a, b) => `${app_root}/data/hrace_count/h-${a}-${b}.json`;
  for (let chunk of _.chunk(hids, cs)) {
    let docs = await zed_db.db
      .collection("horse_stats")
      .find(
        { hid: { $in: chunk } },
        {
          projection: {
            hid: 1,
            "free.n": 1,
            "paid.all.n": 1,
          },
        }
      )
      .toArray();
    let ar = (docs || []).map((e) => {
      let hid = e?.hid || null;
      let f = e?.free?.n || 0;
      let p = e?.paid?.all?.n || 0;
      let n = p + f;
      if (!hid) return null;
      return { hid, f, p, n };
    });
    ar = _.compact(ar);
    let [a, b] = [chunk[0], chunk[chunk.length - 1]];
    write_to_path({
      file_path: fpth(a, b),
      data: ar,
    });
    console.log("chunk", a, b);
  }
  console.table(ar);
  console.log("done");
};

const h_race_final = async () => {
  await init();
  let st = 1;
  let ed = await get_ed_horse();
  let cs = 500;
  let hids = new Array(ed - st + 1).fill(0).map((ea, idx) => st + idx);
  let fpth = (a, b) => `${app_root}/data/hrace_count/h-${a}-${b}.json`;
  let far = [];
  for (let chunk of _.chunk(hids, cs)) {
    let [a, b] = [chunk[0], chunk[chunk.length - 1]];
    let ar = read_from_path({ file_path: fpth(a, b) }) || [];
    far = [...far, ...ar];
  }
  console.log("horses tot:", far.length);
  let farpth = `${app_root}/data/hrace_count/all.json`;
  write_to_path({ file_path: farpth, data: far });
  console.log("done");
};

const gen_races_count_table = async () => {
  let farpth = `${app_root}/data/hrace_count/all.json`;
  let far = read_from_path({ file_path: farpth });
  console.log("horses tot:", far.length);
  let gp = [
    [0, 0],
    [1, 5],
    [5, 10],
  ];
  let ob = gp.map(([mi, mx]) => {
    let ees = _.filter(far, (i) => _.inRange(i?.n || 0, mi, mx + 1e-14));
    let n = ees.length;
    return { mi, mx, n };
  });
  console.table(ob);
};

const runner = async () => {
  gen_races_count_table();
};

runner();
