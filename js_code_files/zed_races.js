const axios = require("axios");
const _ = require("lodash");
const { init, zed_ch } = require("./index-run");
const { struct_race_row_data, delay } = require("./utils");
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

const zed_gql = "https://zed-ql.zed.run/graphql/getRaceResults";

const zed_secret_key = process.env.zed_secret_key;
const mt = 60 * 1000;

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
    // console.log(edges);
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

const add_times_flames_odds = async (raw_data) => {
  let ret = {};
  if (_.isEmpty(raw_data)) return {};
  for (let [rid, race] of _.entries(raw_data)) {
    let raw_race = _.values(race);
    if (_.isEmpty(race)) {
      continue;
    }
    let adj_ob = await get_adjusted_finish_times(rid, "raw_data", raw_race);
    let flames_ob = await get_flames(rid);
    let odds_ob = await get_sims_zed_odds(rid);
    let fee_cat = get_fee_cat_on({ date: raw_race[0][2], fee: raw_race[0][3] });
    ret[rid] = _.chain(raw_data[rid])
      .entries()
      .map(([hid, e]) => {
        e[11] = odds_ob[hid] || 0;
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

const zed_race_add_runner = async (mode = "auto", dates) => {
  // console.log(mode);
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
  let date_str = new Date(now).toISOString().slice(0, 10);
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
      console.log("getting race from:", _.values(r)[0][2], rid);
      min_races.push([rid, r]);
    }
    races = _.chain(min_races).compact().fromPairs().value();
    // console.log(races);

    races = await add_times_flames_odds(races);
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
  zed_race_add_runner("manual", {
    from_a: "2021-09-10T23:27:12Z",
    to_a: "2021-09-10T23:27:12Z",
  });
  // zed_race_add_runner("auto");
  // cron.schedule(cron_str, () => zed_race_add_runner("auto"));
};
zed_races_automated_script_run();

module.exports = {
  zed_secret_key,
  zed_races_automated_script_run,
};
