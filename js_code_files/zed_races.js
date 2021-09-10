const axios = require("axios");
const _ = require("lodash");
const { init, zed_ch } = require("./index-run");
const { struct_race_row_data } = require("./utils");
const {
  get_adjusted_finish_times,
} = require("./zed_races_adjusted_finish_time");
const { get_flames } = require("./zed_races_flames");
const cron = require("node-cron");
const cron_parser = require("cron-parser");
const { get_fee_cat_on } = require("./base");

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

const places_adjusted_times_n_flames = async (raw_data) => {
  for (let [rid, race] of _.entries(raw_data)) {
    let raw_race = _.values(race);
    let adj_ob = await get_adjusted_finish_times(rid, "raw_data", raw_race);
    let flames_ob = await get_flames(rid);
    let fee_cat = get_fee_cat_on({ date: raw_race[0][2], fee: raw_race[0][3] });
    raw_data[rid] = _.chain(raw_data[rid])
      .entries()
      .map(([hid, e]) => {
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

const zed_race_add_runner = async () => {
  let ob = {};
  let now = Date.now();
  const from = new Date(now - mt * 2).toISOString();
  const to = new Date(now).toISOString();
  let date_str = new Date(now).toISOString().slice(0, 10);
  let f_s = new Date(from).toISOString().slice(11, 16);
  let t_s = new Date(to).toISOString().slice(11, 16);
  try {
    console.log(date_str, f_s, "->", t_s);
    let races = await get_zed_raw_data(from, to);
    races = await places_adjusted_times_n_flames(races);
    await push_races_to_mongo(races);
    // console.log(races);
    let rids = _.keys(races);
    console.log(rids.length, "races: ", rids);
  } catch (err) {
    console.log("ERROR on zed_race_add_runner", err.message);
  }
};

const zed_races_automated_script_run = async () => {
  await init();
  console.log("zed_races_script_run started");
  let cron_str = "*/2 * * * *";
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("Next run:", c_itvl.next().toISOString());
  // zed_race_add_runner();
  cron.schedule(cron_str, zed_race_add_runner);
};
// zed_races_script_run();

module.exports = {
  zed_secret_key,
  zed_races_automated_script_run,
};
