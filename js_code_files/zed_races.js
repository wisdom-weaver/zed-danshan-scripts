const axios = require("axios");
const _ = require("lodash");
const { init, zed_ch, zed_db } = require("./index-run");
const {
  struct_race_row_data,
  delay,
  write_to_path,
  fetch_r,
  read_from_path,
} = require("./utils");
const {
  get_adjusted_finish_times,
} = require("./zed_races_adjusted_finish_time");
const { get_flames } = require("./zed_races_flames");
const cron = require("node-cron");
const cron_parser = require("cron-parser");
const {
  get_fee_cat_on,
  download_eth_prices,
  auto_eth_cron,
} = require("./base");
const { get_sims_zed_odds } = require("./sims");
const appRootPath = require("app-root-path");
const { config } = require("dotenv");
const { fetch_a } = require("./fetch_axios");
const {
  add_new_horse_from_zed_in_bulk,
  add_to_new_horses_bucket,
} = require("./zed_horse_scrape");
const {
  update_odds_and_breed_for_race_horses,
} = require("./odds-generator-for-blood2");

const zed_gql = "https://zed-ql.zed.run/graphql/getRaceResults";

// const zed_secret_key = process.env.zed_secret_key;
const zed_secret_key =
  "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJjcnlwdG9maWVsZF9hcGkiLCJleHAiOjE2MzYyMDk4NDMsImlhdCI6MTYzMzc5MDY0MywiaXNzIjoiY3J5cHRvZmllbGRfYXBpIiwianRpIjoiMTY5MzViODgtZWQ1MS00NzlkLThkMWQtMzRlNmFhZTVkYmU0IiwibmJmIjoxNjMzNzkwNjQyLCJzdWIiOnsiZXh0ZXJuYWxfaWQiOiIzMjA4YmVmNy01OTRjLTRhYTgtOGU2YS0zNzJkMTNkY2I2NjMiLCJpZCI6MTQzNzAsInB1YmxpY19hZGRyZXNzIjoiMHhhMGQ5NjY1RTE2M2Y0OTgwODJDZDczMDQ4REExN2U3ZDY5RmQ5MjI0Iiwic3RhYmxlX25hbWUiOiJEYW5zaGFuIn0sInR5cCI6ImFjY2VzcyJ9.b3lw8F5a2BWI3gD3K5ELNc1uBbWp2MVljLFxcrommGCpHG5s1Ue1M19MRu1yVnZc4sgQ4ETa0a3YvsqDjTCwAQ";
const mt = 60 * 1000;
const hr = 1000 * 60 * 60;
const day_diff = 1000 * 60 * 60 * 24 * 1;
const g_h = 72;

const cron_conf = {
  scheduled: true,
};

const def_config = {
  g_odds_zero: true,
  upd_horses: true,
};

const nano_diff = (d1, d2) => {
  d1 = new Date(d1).getTime();
  d2 = new Date(d2).getTime();
  return d1 - d2;
};

const push_to_g_bucket = async (g_bucket = []) => {
  let id = "g_bucket";
  await zed_db.db.collection("script").updateOne(
    { id },
    {
      $set: { id },
      $addToSet: { g_bucket: { $each: g_bucket } },
    },
    { upsert: true }
  );
};
const pull_from_g_bucket = async (g_bucket = []) => {
  let id = "g_bucket";
  await zed_db.db.collection("script").updateOne(
    { id },
    {
      $set: { id },
      $pullAll: { g_bucket: g_bucket },
    },
    { upsert: true }
  );
};
const push_to_need_odds_bucket = async (need_odds_bucket = []) => {
  let id = "need_odds_bucket";
  await zed_db.db.collection("script").updateOne(
    { id },
    {
      $set: { id },
      $addToSet: { need_odds_bucket: { $each: need_odds_bucket } },
    },
    { upsert: true }
  );
};
const pull_from_need_odds_bucket = async (need_odds_bucket = []) => {
  let id = "need_odds_bucket";
  await zed_db.db.collection("script").updateOne(
    { id },
    {
      $set: { id },
      $pullAll: { need_odds_bucket: need_odds_bucket },
    },
    { upsert: true }
  );
};
const push_to_err_bucket = async (err_bucket = []) => {
  let id = "err_bucket";
  await zed_db.db.collection("script").updateOne(
    { id },
    {
      $set: { id },
      $addToSet: { err_bucket: { $each: err_bucket } },
    },
    { upsert: true }
  );
};
const pull_from_err_bucket = async (err_bucket = []) => {
  let id = "err_bucket";
  await zed_db.db.collection("script").updateOne(
    { id },
    {
      $set: { id },
      $pullAll: { err_bucket: err_bucket },
    },
    { upsert: true }
  );
};

const get_zed_raw_data = async (from, to) => {
  try {
    let arr = [];
    let json = {};
    let headers = {
      "x-developer-secret": zed_secret_key,
      "Content-Type": "application/json",
    };

    let payload = {
      query: `query ($input: GetRaceResultsInput, $before: String, $after: String, $first: Int, $last: Int) {
  getRaceResults(before: $before, after: $after, first: $first, last: $last, input: $input) {
    edges {
      cursor
      node {
        name
        length
        startTime
        fee
        raceId
        status
        class

        horses {
          horseId
          finishTime
          finalPosition
          name
          gate
          ownerAddress
          class

        }
      }
    }
  }
}`,
      variables: {
        first: 500,
        input: {
          dates: {
            from: from,
            to: to,
          },
        },
      },
    };

    let axios_config = {
      method: "post",
      url: zed_gql,
      headers: headers,
      data: JSON.stringify(payload),
    };
    let result = await axios(axios_config);
    let edges = result?.data?.data?.getRaceResults?.edges || [];
    let racesData = {};
    for (let edgeIndex in edges) {
      let edge = edges[edgeIndex];
      let node = edge.node;
      let node_length = node.length;
      let node_startTime = node.startTime;
      let node_fee = node.fee;
      let node_raceId = node.raceId;
      let node_class = node.class;
      let horses = node.horses;

      racesData[node_raceId] = {};
      for (let horseIndex in horses) {
        let horse = horses[horseIndex];
        let horses_horseId = horse.horseId;
        let horses_finishTime = horse.finishTime;
        let horses_finalPosition = horse.finalPosition;
        let horses_name = horse.name;
        let horses_gate = horse.gate;
        let horses_class = horse.class;

        racesData[node_raceId][horses_horseId] = {
          1: node_length,
          2: node_startTime,
          3: node_fee,
          4: node_raceId,
          5: node_class,
          6: horses_horseId,
          7: horses_finishTime,
          8: horses_finalPosition,
          9: horses_name,
          10: horses_gate,
          11: 0,
          12: 0,
          16: horses_class,
        };
      }
    }

    return racesData;
  } catch (err) {
    console.log("err", err);
    return [];
  }
};

const add_times_flames_odds_to_1race = async ([rid, race], config) => {
  let raw_race = _.values(race);
  if (_.isEmpty(race)) {
    return [rid, null];
  }

  let date = raw_race[0][2];
  let thisclass = raw_race[0][5];
  // console.log(rid, 1);
  let adj_ob = await get_adjusted_finish_times(rid, "raw_data", raw_race);
  // console.log(rid, 2);
  let flames_ob = await get_flames(rid);
  // console.log(rid, 3);
  let odds_ob = {};
  if (!_.isEmpty(config) && config.g_odds_zero == true) {
    if (thisclass == 0) odds_ob = {};
    else odds_ob = await get_sims_zed_odds(rid, "raw_race", raw_race);
  } else odds_ob = await get_sims_zed_odds(rid, "raw_race", raw_race);
  // console.log(rid, 4);
  // console.log(rid, thisclass, odds_ob);
  // console.log(date, date >= "2021-08-24T00:00:00.000Z");
  let fee_cat = get_fee_cat_on({ date, fee: raw_race[0][3] });
  let modified = _.chain(race)
    .entries()
    .map(([hid, e]) => {
      if (date >= "2021-08-24T00:00:00.000Z") {
        // console.log(hid, thisclass, odds_ob[hid]);
        e[11] = odds_ob[hid] || 0;
      } else delete e[11];
      e[13] = flames_ob[hid];
      e[14] = fee_cat;
      e[15] = adj_ob[hid];
      return [hid, e];
    })
    .fromPairs()
    .value();
  return [rid, modified];
};

const add_times_flames_odds_to_races = async (raw_data, config) => {
  // let ret = {};
  if (_.isEmpty(raw_data)) return {};
  let ret = [];
  let cs = 5;
  for (let data_chunk of _.chunk(_.entries(raw_data), cs)) {
    let this_chunk = await Promise.all(
      data_chunk.map(([rid, race]) =>
        add_times_flames_odds_to_1race([rid, race], config)
      )
    );
    // console.log(this_chunk)
    ret = [...ret, ...this_chunk];
  }
  ret = _.chain(ret)
    .filter((i) => i[1] !== null)
    .fromPairs()
    .value();
  // console.log(ret);

  // for (let [rid, race] of _.entries(raw_data)) {
  //   let raw_race = _.values(race);
  //   if (_.isEmpty(race)) {
  //     continue;
  //   }

  //   let date = raw_race[0][2];
  //   let thisclass = raw_race[0][5];
  //   console.log(rid, 1);
  //   let adj_ob = await get_adjusted_finish_times(rid, "raw_data", raw_race);
  //   console.log(rid, 2);
  //   let flames_ob = await get_flames(rid);
  //   console.log(rid, 3);
  //   let odds_ob = {};
  //   if (!_.isEmpty(config) && config.g_odds_zero == true) {
  //     if (thisclass == 0) odds_ob = {};
  //     else odds_ob = await get_sims_zed_odds(rid, "raw_race", raw_race);
  //   } else odds_ob = await get_sims_zed_odds(rid, "raw_race", raw_race);
  //   console.log(rid, 4);
  //   // console.log(rid, thisclass, odds_ob);
  //   // console.log(date, date >= "2021-08-24T00:00:00.000Z");
  //   let fee_cat = get_fee_cat_on({ date, fee: raw_race[0][3] });
  //   ret[rid] = _.chain(raw_data[rid])
  //     .entries()
  //     .map(([hid, e]) => {
  //       if (date >= "2021-08-24T00:00:00.000Z") {
  //         // console.log(hid, thisclass, odds_ob[hid]);
  //         e[11] = odds_ob[hid] || 0;
  //       } else delete e[11];
  //       e[13] = flames_ob[hid];
  //       e[14] = fee_cat;
  //       e[15] = adj_ob[hid];
  //       return [hid, e];
  //     })
  //     .fromPairs()
  //     .value();
  //   console.log("raw_data", rid, raw_data[rid]);
  //   console.log("ret", rid, ret[rid]);
  // }
  // console.log(raw_data)
  // return raw_data;
  return ret;
};

const push_races_to_mongo = async (races) => {
  if (_.isEmpty(races)) return;
  let mongo_push = [];
  for (let r in races) {
    for (let h in races[r]) {
      mongo_push.push({
        updateOne: {
          filter: { 4: races[r][h]["4"], 6: races[r][h]["6"] },
          update: { $set: { ...races[r][h] } },
          upsert: true,
        },
      });
    }
  }
  // console.log(mongo_push);
  await zed_ch.db.collection("zed").bulkWrite(mongo_push);
};

const handle_racing_horse = async (horses) => {
  let hids = _.chain(horses)
    .keys()
    .map((i) => parseInt(i))
    .value();
  console.log("handle_racing_horse", hids.length);
  let exst_docs = await zed_db.db
    .collection("horse_details")
    .find({ hid: { $in: hids } }, { projection: { hid: 1, _id: 0 } })
    .toArray();
  let exst_hids = _.map(exst_docs, "hid") || [];
  let diff_hids = _.difference(hids, exst_hids);
  console.log("new horses:", diff_hids);
  if (!_.isEmpty(diff_hids)) {
    // console.log("new horses:", diff_hids);
    // await add_new_horse_from_zed_in_bulk(diff_hids);
    await add_to_new_horses_bucket(diff_hids);
  }
};

const preprocess_races_pushing_to_mongo = async (
  races,
  config = def_config
) => {
  if (_.isEmpty(races)) return;
  let ar = [];
  for (let r in races) {
    for (let h in races[r]) {
      ar.push([races[r][h][6], races[r][h][16]]);
    }
  }
  // await handle_racing_horse(ar);
  await push_races_to_mongo(races);
  console.log("PUSHED", _.keys(races).length, "races to mongo");
  if (config.upd_horses) {
    await zed_db.db.collection("script").updateOne(
      { id: "racing_horses" },
      {
        $set: { id: "racing_horses" },
        $addToSet: { racing_horses: { $each: ar } },
      },
      { upsert: true }
    );
    console.log(ar.length, "race_horses to be eval");
  }
};

const zed_race_add_runner = async (
  mode = "auto",
  dates,
  config = def_config
) => {
  // console.log(mode);
  let err_bucket = [];
  let g_bucket = [];
  let offset_delay = 3 * 60 * 1000;
  let ob = {};
  let now = Date.now() - offset_delay;
  let from, to;
  if (mode == "auto") {
    from = new Date(now - mt * 1.2).toISOString();
    to = new Date(now).toISOString();
  } else if (mode == "manual") {
    let { from_a, to_a } = dates || {};

    from = new Date(from_a).toISOString();
    to = new Date(to_a).toISOString();
  }
  let date_str = new Date(from).toISOString().slice(0, 10);
  let f_s = new Date(from).toISOString().slice(11, 19);
  let t_s = new Date(to).toISOString().slice(11, 19);
  try {
    console.log("\n#", date_str, f_s, "->", t_s);
    let races = await get_zed_raw_data(from, to);
    if (_.keys(races).length == 0) {
      console.log("no races");
      return;
    }

    let min_races = [];
    let rids = _.keys(races);
    console.log("fetched", rids.length, "race ids");
    let doc_exists = [];
    if (mode == "auto2")
      doc_exists = await Promise.all(
        rids.map((rid) =>
          zed_ch.db.collection("zed").findOne({ 4: rid }, { _id: 0, 4: 1 })
        )
      );
    // console.log(doc_exists);
    doc_exists = _.chain(doc_exists)
      .map((it, idx) => {
        if (_.isEmpty(it)) return [rids[idx], false];
        else return [rids[idx], true];
      })
      .fromPairs()
      .value();
    console.log(doc_exists);

    for (let [rid, r] of _.entries(races)) {
      if (_.isEmpty(r)) {
        console.log("err", rid, "empty race");
        continue;
      }
      if (doc_exists[rid]) {
        console.log("existing race at :", _.values(r)[0][2], rid);
        continue;
      }

      let l = _.values(r).length;
      if (l == 0) {
        console.log("ERROR EMPTY       ", _.values(r)[0][2], rid, `${l}_h`);
        err_bucket.push({ rid, date: null });
      } else if (l < 12) {
        console.log("ERROR             ", _.values(r)[0][2], rid, `${l}_h`);
        err_bucket.push({ rid, date: _.values(r)[0][2] });
        let thisclass = _.values(r)[0][5];
        if (thisclass == 0) g_bucket.push({ rid, date: _.values(r)[0][2] });
      } else {
        console.log("getting race from:", _.values(r)[0][2], rid, `${l}_h`);
        min_races.push([rid, r]);
      }
    }
    await push_to_err_bucket(err_bucket);
    await push_to_g_bucket(g_bucket);
    races = _.chain(min_races).compact().fromPairs().value();
    // console.log(races);

    races = await add_times_flames_odds_to_races(races, config);
    // console.log(races)
    await preprocess_races_pushing_to_mongo(races, config);
    console.log("#DONE", _.keys(races).length, "races\n---");
  } catch (err) {
    console.log("ERROR on zed_race_add_runner", err.message);
  }
};

const zed_races_specific_duration_run = async () => {
  await init();
  await auto_eth_cron();
  await delay(1000);
  console.log("\n## zed_races_specific_duration_run started");

  let from_a, to_a;
  // console.log("Enter date in format Eg: 2021-09-11T00:13Z: ");
  // from_a = prompt("from: ");
  // to_a = prompt("to  : ");

  // let date_st = "2021-08-24T00:00:00Z";
  // let date_ed = new Date().toISOString();
  let date = "2021-10-18Z";
  let date_st = "2021-10-18Z";
  date = "2021-10-22T18:18:37.796Z";
  let date_ed = date;
  // let date_ed = new Date().toISOString();

  from_a = date_st;
  to_a = date_ed;

  from_a = new Date(from_a).getTime() - 1;
  to_a = new Date(to_a).getTime() + 1;

  while (from_a < to_a) {
    let to_a_2 = Math.min(to_a, from_a + 10 * mt);
    let conf = def_config;
    conf.upd_horses = false;
    await zed_race_add_runner("manual", { from_a, to_a: to_a_2 }, conf);
    from_a = to_a_2;
  }
  console.log("ended");
};
// zed_races_specific_duration_run();

const zed_races_since_last_run = async () => {
  await init();
  await auto_eth_cron();
  await delay(1000);
  console.log("\n## zed_races_since_last_run started");
  let from_a, to_a;
  let now = Date.now();
  let last_doc = await zed_ch.db
    .collection("zed")
    .find({})
    .sort({ 2: -1 })
    .limit(5)
    .toArray();
  from_a = last_doc[2][2];
  console.log("last doc date: ", from_a);

  from_a = new Date(from_a).getTime() - 5 * mt;
  while (from_a < Date.now() - mt) {
    to_a = from_a + 5 * mt - 1;
    await zed_race_add_runner(
      "manual",
      { from_a, to_a },
      { ...def_config, upd_horses: false }
    );
    from_a = to_a;
  }
  // await zed_races_err_runner();
  // await zed_races_g_runner();
  // zed_races_automated_script_run("auto");
};
// zed_races_since_last_run();

const zed_results_data = async (rid) => {
  let api = `https://racing-api.zed.run/api/v1/races/result/${rid}`;
  let doc = await fetch_a(api);
  if (_.isEmpty(doc)) return null;
  let { horse_list = [] } = doc;
  let ob = _.chain(horse_list)
    .map((i) => {
      let { finish_position, gate, id, name, time } = i;
      return {
        hid: id,
        name,
        gate,
        place: finish_position,
        finishtime: time,
      };
    })
    .keyBy("hid")
    .value();
  return ob;
};
const zed_flames_data = async (rid) => {
  let api = `https://rpi.zed.run/?race_id=${rid}`;
  let doc = await fetch_a(api);
  if (_.isEmpty(doc)) return null;
  let { rpi } = doc;
  return rpi;
};
const zed_race_base_data = async (rid) => {
  let api = `https://racing-api.zed.run/api/v1/races?race_id=${rid}`;
  let doc = await fetch_a(api);
  // console.log("base raw", doc);
  if (_.isEmpty(doc)) return null;
  let {
    class: thisclass,
    fee: entryfee,
    gates,
    length: distance,
    start_time: date,
    status,
  } = doc;
  if (status !== "finished") return null;
  let hids = _.values(gates);
  if (!date.endsWith("Z")) date += "Z";
  let ob = { rid, thisclass, hids, entryfee, distance, date };
  // console.log("base struct", ob);
  return ob;
};

const zed_race_build_for_mongodb = async (rid, conf = {}) => {
  let { mode = "g" } = conf;
  let [base, results, flames] = await Promise.all([
    zed_race_base_data(rid),
    zed_results_data(rid),
    zed_flames_data(rid),
  ]);
  if (_.isEmpty(base)) {
    console.log("couldnt get race", rid);
    return [];
  }
  let { hids, thisclass, entryfee, distance, date } = base;
  let fee_cat = get_fee_cat_on({ date, fee: entryfee });
  let ar = hids.map((hid) => {
    return {
      hid,
      ...results[hid],
      flame: flames[hid],
    };
  });
  ar = ar.map((i) => ({
    1: distance,
    2: date,
    3: entryfee,
    4: rid,
    5: thisclass,
    6: i.hid,
    7: i.finishtime,
    8: i.place,
    9: i.name,
    10: i.gate,
    11: 0,
    12: 0,
    13: i.flame,
    14: fee_cat,
    15: 0,
  }));
  let adj_ob = await get_adjusted_finish_times(rid, "raw_data", ar);
  // console.log(rid, adj_ob);

  ar = ar.map((i) => ({ ...i, 15: adj_ob[i[6]] }));
  if (mode == "g" || (mode == "err" && thisclass !== 0)) {
    let odds_ob = await get_sims_zed_odds(rid, "raw_race", ar);
    ar = ar.map((i) => ({ ...i, 11: odds_ob[i[6]] }));
    // console.log(mode, thisclass, odds_ob);
  }
  if (mode == "err" && thisclass == 0) {
    console.log("G", { rid, date });
    await push_to_g_bucket([{ rid, date }]);
  }
  ar = _.keyBy(ar, "6");
  return ar;
};

const zed_g_runner_single_race = async (rid) => {
  if (!rid) return;
  return zed_race_build_for_mongodb(rid, { mode: "g" });
};

const zed_err_runner_single_race = async (rid) => {
  if (!rid) return;
  return zed_race_build_for_mongodb(rid, { mode: "err" });
};

const zed_race_odds_struct_mongodb = async (rid) => {
  let ob = await get_sims_zed_odds(rid);
  ob = _.entries(ob).map(([hid, odds]) => [
    hid,
    { 4: rid, 6: parseFloat(hid), 11: odds },
  ]);
  ob = _.fromPairs(ob);
  return ob;
};

const zed_races_g_runner = async () => {
  let g_docs = await zed_db.db.collection("script").findOne({ id: "g_bucket" });
  g_docs = g_docs?.g_bucket || [];
  g_docs = _.sortBy(g_docs, "date");

  let cs = 1;
  console.log("g_docs", g_docs.length);
  for (let chunk of _.chunk(g_docs, cs)) {
    try {
      let err_s = [];
      let rem_g_s = [];

      let data = await Promise.all(
        chunk.map(({ rid, date }) => {
          if (nano_diff(Date.now(), date) < g_h * hr) return [rid, -1];
          return zed_g_runner_single_race(rid).then((d) => [rid, d]);
        })
      );

      data = data.filter(([rid, r]) => {
        let this_doc = _.find(chunk, { rid });
        if (r == -1) {
          console.log(
            "g_odds",
            this_doc.date,
            rid,
            `SKIPPED should be ${g_h}hrs old`
          );
          return false;
        }
        let this_len = _.values(r).length;
        console.log("g_odds", this_doc.date, rid, `${this_len}_h`);
        if (_.isEmpty(r) || this_len < 12) {
          err_s.push(this_doc);
          return false;
        } else {
          rem_g_s.push(this_doc);
          return true;
        }
      });
      let len = data.length;
      if (!_.isEmpty(data)) {
        data = _.fromPairs(data);
        await preprocess_races_pushing_to_mongo(data, def_config);
        console.log("g_odds", len, "races pushed");
      } else console.log("g_odds", "NO races pushed");
      if (!_.isEmpty(rem_g_s)) {
        await pull_from_g_bucket(rem_g_s);
        console.log(
          "g_odds",
          rem_g_s.length,
          "SUCCESS races rem from g_bucket & add to need_odds_bucket"
        );
      }
      if (!_.isEmpty(err_s)) {
        await push_to_g_bucket(err_s);
        console.log(
          "g_odds",
          err_s.length,
          "ERROR found and pushed to g_bucket"
        );
      }
      console.log("\n");
    } catch (err) {
      console.log("ERROR in zed_races_g_runner", err.message);
    }
    await delay(4000);
  }
};
const zed_races_g_manual_run = async () => {
  await init();
  await zed_races_g_runner();
  console.log("completed zed_g_races");
};

const zed_races_err_runner = async () => {
  let err_docs = await zed_db.db
    .collection("script")
    .findOne({ id: "err_bucket" });
  err_docs = err_docs?.err_bucket || [];
  err_docs = _.sortBy(err_docs, "date");

  let cs = 1;
  console.log("err_docs", err_docs.length);
  // console.table(err_docs.slice(0,10)); ;
  // return;
  for (let chunk of _.chunk(err_docs, cs)) {
    try {
      let err_s = [];
      let rem_err_s = [];

      let data = await Promise.all(
        chunk.map(({ rid, date }) => {
          return zed_err_runner_single_race(rid).then((d) => [rid, d]);
        })
      );

      data = data.filter(([rid, r]) => {
        let this_doc = _.find(chunk, { rid });
        let this_len = _.values(r).length;
        console.log("err_race", this_doc.date, rid, `${this_len}_h`);
        if (_.isEmpty(r) || this_len < 12) {
          err_s.push(this_doc);
          return false;
        } else {
          rem_err_s.push(this_doc);
          return true;
        }
      });
      let len = data.length;
      if (!_.isEmpty(data)) {
        data = _.fromPairs(data);
        await preprocess_races_pushing_to_mongo(data, def_config);
        console.log(len, "races pushed");
      } else console.log("NO races pushed");
      if (!_.isEmpty(rem_err_s)) {
        await pull_from_err_bucket(rem_err_s);
        console.log(rem_err_s.length, "SUCCESS races rem from err_bucket");
      }
      if (!_.isEmpty(err_s)) {
        await push_to_err_bucket(err_s);
        console.log(err_s.length, "ERROR found and pushed to err_bucket");
      }
      console.log("\n");
    } catch (err) {
      console.log("ERROR in zed_races_err_runner", err.message);
    }
    await delay(5000);
  }
};
const zed_races_err_manual_run = async () => {
  await init();
  await zed_races_err_runner();
  console.log("completed zed_err_races");
};

const zed_races_scripts_init = async () => {
  await init();
  await auto_eth_cron();
  await delay(1000);
};

const zed_races_g_auto_run = async () => {
  await zed_races_scripts_init();
  console.log("\n## zed_races_g_auto_run started");

  let cron_str_1 = "0 0 * * *";
  const c_itvl_1 = cron_parser.parseExpression(cron_str_1);
  console.log("Next g_races RUN:", c_itvl_1.next().toISOString());
  cron.schedule(cron_str_1, () => zed_races_g_runner(), cron_conf);
};

const zed_races_err_auto_run = async () => {
  await zed_races_scripts_init();
  console.log("\n## zed_races_err_auto_run started");

  let cron_str_1 = "*/20 * * * *";
  const c_itvl_1 = cron_parser.parseExpression(cron_str_1);
  console.log("Next err_races RUN:", c_itvl_1.next().toISOString());
  cron.schedule(cron_str_1, () => zed_races_err_runner(), cron_conf);
};

const zed_races_automated_script_run = async () => {
  await zed_races_scripts_init();
  console.log("\n## zed_races_script_run started");
  let cron_str = "*/1 * * * *";
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("Next run:", c_itvl.next().toISOString(), "\n");
  cron.schedule(
    cron_str,
    () => zed_race_add_runner("auto", {}, def_config),
    cron_conf
  );
  // zed_race_add_runner("auto", def_config);
  // zed_races_g_auto_run();
  // zed_races_err_auto_run();
};
// zed_races_automated_script_run();

const get_zed_gql_rids = async (from, to) => {
  try {
    let arr = [];
    let json = {};
    let headers = {
      "x-developer-secret": zed_secret_key,
      "Content-Type": "application/json",
    };

    let payload = {
      query: `query ($input: GetRaceResultsInput, $before: String, $after: String, $first: Int, $last: Int) {
  getRaceResults(before: $before, after: $after, first: $first, last: $last, input: $input) {
    edges {
      cursor
      node {
        startTime
        raceId
      }
    }
  }
}`,
      variables: {
        first: 5000000,
        input: {
          dates: {
            from: from,
            to: to,
          },
        },
      },
    };

    let axios_config = {
      method: "post",
      url: zed_gql,
      headers: headers,
      data: JSON.stringify(payload),
    };
    let result = await axios(axios_config);
    let edges = result?.data?.data?.getRaceResults?.edges || [];
    let rids = [];
    for (let edgeIndex in edges) {
      let edge = edges[edgeIndex];
      let node = edge.node;
      let node_raceId = node.raceId;
      rids.push(node_raceId);
    }
    return rids;
  } catch (err) {
    console.log("err", err);
    return [];
  }
};

const zed_races_get_missings = async () => {
  await init();
  let cs = 10;
  let now = Date.now() - 5 * 60 * 1000;
  let from = new Date(now - 3 * day_diff).toISOString();
  let to = new Date(now).toISOString();
  const rids_all = await get_zed_gql_rids(from, to);
  console.log("#", from, "->", to);
  console.log("total   :", rids_all.length);
  let docs = await zed_ch.db
    .collection("zed")
    .find({ 2: { $gt: from, $lt: to } }, { projection: { _id: 0, 4: 1 } })
    .toArray();
  let rids_exists = _.chain(docs).map("4").uniq().compact().value();
  console.log("existing:", rids_exists.length);
  let rids = _.difference(rids_all, rids_exists);
  // let rids = ["c6laoIZ", "zKEDb9my", "7XPm0eN6", "LpynTBUq", "UxYk4KWO"];
  console.log("missing:", rids.length);
  for (let chunk of _.chunk(rids, cs)) {
    let err_s = [];
    let rem_err_s = [];
    console.log(chunk);
    let data = await Promise.all(
      chunk.map((rid) => {
        return zed_err_runner_single_race(rid).then((d) => [rid, d]);
      })
    );

    // console.log(data);

    data = data.filter(([rid, r]) => {
      let this_doc = _.find(chunk, { rid });
      let this_len = _.values(r).length;
      console.log("missing_race", rid, `${this_len}_h`);
      if (_.isEmpty(r) || this_len < 12) {
        err_s.push(this_doc);
        return false;
      } else {
        rem_err_s.push(this_doc);
        return true;
      }
    });
    let len = data.length;
    if (!_.isEmpty(data)) {
      data = _.fromPairs(data);
      await preprocess_races_pushing_to_mongo(data, def_config);
      console.log(len, "races pushed");
    } else console.log("NO races pushed");
  }
  console.log("completed");
};

const runner = async () => {
  await init();
  await zed_races_get_missings();
  // zed_race_add_runner("auto", {}, def_config);
  console.log("done");
};
// runner();

module.exports = {
  zed_secret_key,
  zed_races_automated_script_run,
  zed_races_specific_duration_run,
  zed_races_since_last_run,
  zed_races_err_manual_run,
  zed_races_g_manual_run,
  zed_races_get_missings,
  zed_races_g_auto_run,
  zed_races_err_auto_run,
};
