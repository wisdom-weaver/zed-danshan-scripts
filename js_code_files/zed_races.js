const axios = require("axios");
const _ = require("lodash");
const { init, zed_ch, zed_db } = require("./index-run");
const {
  struct_race_row_data,
  delay,
  write_to_path,
  fetch_r,
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
var prompt = require("prompt-sync")();

const zed_gql = "https://zed-ql.zed.run/graphql/getRaceResults";

const zed_secret_key = process.env.zed_secret_key;
const mt = 60 * 1000;
const hr = 1000 * 60 * 60;
const day_diff = 1000 * 60 * 60 * 24 * 1;

const def_config = {
  g_odds_zero: true,
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
        };
      }
    }

    return racesData;
  } catch (err) {
    console.log("err", err);
    return [];
  }
};

const add_times_flames_odds_to_races = async (raw_data, config) => {
  let ret = {};
  if (_.isEmpty(raw_data)) return {};
  for (let [rid, race] of _.entries(raw_data)) {
    let raw_race = _.values(race);
    if (_.isEmpty(race)) {
      continue;
    }

    let date = raw_race[0][2];
    let thisclass = raw_race[0][5];

    let adj_ob = await get_adjusted_finish_times(rid, "raw_data", raw_race);
    let flames_ob = await get_flames(rid);
    let odds_ob = {};
    if (!_.isEmpty(config) && config.g_odds_zero == true) {
      if (thisclass == 0) odds_ob = {};
      else odds_ob = await get_sims_zed_odds(rid);
    } else odds_ob = await get_sims_zed_odds(rid);
    // console.log(rid, thisclass, odds_ob);

    let fee_cat = get_fee_cat_on({ date, fee: raw_race[0][3] });
    ret[rid] = _.chain(raw_data[rid])
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
  }
  return raw_data;
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

const zed_race_add_runner = async (mode = "auto", dates, config) => {
  // console.log(mode);
  let err_bucket = [];
  let g_bucket = [];

  let ob = {};
  let now = Date.now();
  let from, to;
  if (mode == "auto") {
    from = new Date(now - mt * 5).toISOString();
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
    if (mode == "auto")
      doc_exists = await Promise.all(
        rids.map((rid) => zed_ch.db.collection("zed").findOne({ 4: rid }))
      );

    doc_exists = _.chain(doc_exists)
      .map((it, idx) => {
        let rid = rids[idx];
        if (_.isEmpty(it)) return [rid, false];
        else return [rid, true];
      })
      .fromPairs()
      .value();
    // console.log(doc_exists);

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
    await push_races_to_mongo(races);
    console.log("done", _.keys(races).length, "races");
  } catch (err) {
    console.log("ERROR on zed_race_add_runner", err.message);
  }
};

const zed_races_automated_script_run = async () => {
  await init();
  await auto_eth_cron();
  await delay(1000);
  console.log("\n## zed_races_script_run started");
  let cron_str = "*/2 * * * *";
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("Next run:", c_itvl.next().toISOString(), "\n");
  cron.schedule(cron_str, () => zed_race_add_runner("auto", def_config));
};
// zed_races_automated_script_run();

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
  let date = "2021-08-24T15:40:49Z";
  let date_st = date;
  let date_ed = date;

  from_a = date_st;
  to_a = date_ed;

  from_a = new Date(from_a).getTime() - 1;
  to_a = new Date(to_a).getTime() + 1;

  while (from_a < to_a) {
    let to_a_2 = Math.min(to_a, from_a + 30 * mt);
    await zed_race_add_runner("manual", { from_a, to_a: to_a_2 }, def_config);
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
    .limit(1)
    .toArray();
  from_a = last_doc[0][2];
  console.log("last doc date: ", from_a);

  from_a = new Date(from_a).getTime() - 10 * mt;
  while (from_a < Date.now() - mt) {
    to_a = from_a + 10 * mt;
    await zed_race_add_runner("manual", { from_a, to_a }, def_config);
    from_a = to_a;
  }
  zed_races_automated_script_run("auto");
};
// zed_races_since_last_run();

const zed_results_data = async (rid) => {
  let api = `https://racing-api.zed.run/api/v1/races/result/${rid}`;
  let doc = await fetch_r(api);
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
  let doc = await fetch_r(api);
  if (_.isEmpty(doc)) return null;
  let { rpi } = doc;
  return rpi;
};
const zed_race_base_data = async (rid) => {
  let api = `https://racing-api.zed.run/api/v1/races?race_id=${rid}`;
  let doc = await fetch_r(api);
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
  let ob = { rid, thisclass, hids, entryfee, distance, date };
  return ob;
};

const zed_race_build_for_mongodb = async (rid, conf = {}) => {
  let { with_odds = false } = conf;
  let [base, results, flames] = await Promise.all([
    zed_race_base_data(rid),
    zed_results_data(rid),
    zed_flames_data(rid),
  ]);
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
  if (with_odds == true) {
    let odds_ob = await get_sims_zed_odds(rid);
    ar = ar.map((i) => ({ ...i, 11: odds_ob[i[6]] }));
    // console.log(odds_ob);
  }
  ar = _.keyBy(ar, "6");
  return ar;
};

const zed_g_runner_single_race = async (rid) => {
  if (!rid) return;
  return zed_race_build_for_mongodb(rid, { with_odds: false });
};

const zed_race_odds_struct_mongodb = async (rid) => {
  let ob = await get_sims_zed_odds(rid);
  ob = _.entries(ob).map(([hid, odds]) => [hid, { 11: odds }]);
  ob = _.fromPairs(ob);
  return ob;
};

const zed_races_g_runner = async () => {
  await init();
  let g_docs = await zed_db.db.collection("script").findOne({ id: "g_bucket" });
  g_docs = g_docs?.g_bucket || [];
  let cs = 5;
  console.log("g_docs", g_docs.length);
  for (let chunk of _.chunk(g_docs, cs)) {
    try {
      let err_s = [];
      let rem_g_s = [];

      let data = await Promise.all(
        chunk.map(({ rid, date }) => {
          return zed_g_runner_single_race(rid).then((d) => [rid, d]);
        })
      );

      data = data.filter(([rid, r]) => {
        let this_doc = _.find(chunk, { rid });
        let this_len = _.values(r).length;
        console.log("g_race", this_doc.date, rid, `${this_len}_h`);
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
        await push_races_to_mongo(data);
        console.log(len, "races pushed");
      } else console.log("NO races pushed");
      if (!_.isEmpty(rem_g_s)) {
        await pull_from_g_bucket(rem_g_s);
        await push_to_need_odds_bucket(rem_g_s);
        console.log(
          rem_g_s.length,
          "SUCCESS races rem from g_bucket & add to need_odds_bucket"
        );
      }
      if (!_.isEmpty(err_s)) {
        await push_to_g_bucket(err_s);
        console.log(err_s.length, "ERROR found and pushed to g_bucket");
      }
      console.log("\n");
    } catch (err) {
      console.log("ERROR in zed_races_g_runner", err.message);
    }
  }
};
const zed_races_g_odds_runner = async () => {
  await init();

  let cs = 5;
  let g_h = 72;

  let need_odds_docs = await zed_db.db
    .collection("script")
    .findOne({ id: "need_odds_bucket" });
  need_odds_docs = need_odds_docs?.need_odds_bucket || [];

  console.log("need_odds_docs", need_odds_docs.length);
  for (let chunk of _.chunk(need_odds_docs, cs)) {
    try {
      let err_s = [];
      let rem_n_s = [];

      let data = await Promise.all(
        chunk.map(({ rid, date }) => {
          if (Math.abs(nano_diff(date, Date.now())) < g_h * hr) return -1;
          return zed_race_odds_struct_mongodb(rid).then((d) => [rid, d]);
        })
      );
      
      data = data.filter(([rid, r]) => {
        let this_doc = _.find(chunk, { rid });
        if (r == -1) {
          console.log("g_odds", this_doc.date, rid, `SKIPPED no older than ${g_h}hrs`);
          return false;
        }
        let this_len = _.values(r).length;
        console.log("g_odds", this_doc.date, rid, `${this_len}_h`);
        if (_.isEmpty(r) || this_len < 12) {
          err_s.push(this_doc);
          return false;
        } else {
          rem_n_s.push(this_doc);
          return true;
        }
      });
      let len = data.length;
      // console.log("len:", len);
      if (!_.isEmpty(data)) {
        data = _.fromPairs(data);
        await push_races_to_mongo(data);
        console.log(len, "races odds pushed");
      } else console.log("NO races odds pushed");
      if (!_.isEmpty(rem_n_s)) {
        await pull_from_need_odds_bucket(rem_n_s);
        console.log(
          rem_n_s.length,
          "SUCCESS races odds & rem from need_odds_bucket"
        );
      }
      if (!_.isEmpty(err_s)) {
        await push_to_need_odds_bucket(err_s);
        console.log(err_s.length, "ERROR found and pushed to need_odds_bucket");
      }
      console.log("\n");
    } catch (err) {
      console.log("ERROR in zed_races_g_runner", err.message);
    }
  }
};

const runner = async () => {
  // await init();

  // await zed_ch.db.collection("zed").deleteMany({ 2: "2021-08-24T16:22:49Z" });
  // widtd86c 12_h
  // I6YvLI9r 12_h
  // FNo8fjj7 12_h

  // let dc = { rid: "I6YvLI9r", date: "2021-08-24T15:40:49Z" };
  // let ob = await zed_race_build_for_mongodb(dc.rid, { with_odds: true });
  // console.table(ob);

  // await zed_races_g_runner();
  // await zed_races_g_odds_runner();

  // zed_races_specific_duration_run();
  // console.log("done");
};
// runner();

module.exports = {
  zed_secret_key,
  zed_races_automated_script_run,
  zed_races_specific_duration_run,
  zed_races_since_last_run,
};
