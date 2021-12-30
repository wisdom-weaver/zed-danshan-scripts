const axios = require("axios");
const _ = require("lodash");
const { init, zed_ch, zed_db } = require("../connection/mongo_connect");
const { delay, iso, nano } = require("../utils/utils");
const {
  get_adjusted_finish_times,
} = require("./zed_races_adjusted_finish_time");
const { get_fee_cat_on } = require("../utils/base");
const { get_sims_zed_odds } = require("./sims");
const zedf = require("../utils/zedf");
const race_horses = require("./race_horses");

const zed_gql = "https://zed-ql.zed.run/graphql/getRaceResults";
const zed_secret_key = process.env.zed_secret_key;
let test_mode = 0;

const mt = 60 * 1000;
const hr = 1000 * 60 * 60;
const day_diff = 1000 * 60 * 60 * 24 * 1;
const g_h = 72;

let push_race_horses_on = 0;

const cron_conf = {
  scheduled: true,
};

const def_config = {
  g_odds_zero: true,
  upd_horses: true,
};
let config = def_config;

const nano_diff = (d1, d2) => {
  d1 = new Date(d1).getTime();
  d2 = new Date(d2).getTime();
  return d1 - d2;
};

const get_zed_raw_data = async (from, to) => {
  try {
    let arr = [];
    let json = {};
    let headers = {
      "Content-Type": "application/json",
      Authorization: `Bearer ${zed_secret_key}`,
      Cookie:
        "__cf_bm=tEjKpZDvjFiRn.tUIx1TbiSLPLfAmtzyUWnQo6VHP7I-1636398985-0-ARRsf8lodPXym9lS5lNpyUbf3Hz4a6TJovc1m+sRottgtEN/MoOiOpoNcpW4I0wcA0q4VwQdEKi7Q8VeW8amlWA=",
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
const get_zed_rids_only = async (from, to) => {
  try {
    from = iso(from);
    to = iso(to);
    let arr = [];
    let json = {};
    let headers = {
      "Content-Type": "application/json",
      Authorization: `Bearer ${zed_secret_key}`,
      Cookie:
        "__cf_bm=tEjKpZDvjFiRn.tUIx1TbiSLPLfAmtzyUWnQo6VHP7I-1636398985-0-ARRsf8lodPXym9lS5lNpyUbf3Hz4a6TJovc1m+sRottgtEN/MoOiOpoNcpW4I0wcA0q4VwQdEKi7Q8VeW8amlWA=",
    };
    let payload = {
      query: `query ($input: GetRaceResultsInput, $before: String, $after: String, $first: Int, $last: Int) {
  getRaceResults(before: $before, after: $after, first: $first, last: $last, input: $input) {
    edges {
      cursor
      node {
        startTime
        raceId
        status
      }
    }
  }
}`,
      variables: {
        first: 20000,
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
    let racesData = [];
    racesData = _.map(edges, (e) => e.node.raceId);
    return racesData;
  } catch (err) {
    console.log("err", err);
    return [];
  }
};

const get_zed_ch_rids_only = async (from, to) => {
  try {
    from = iso(from);
    to = iso(to);
    let docs =
      (await zed_ch.db
        .collection("zed")
        .find({ 2: { $gte: from, $lte: to } }, { projection: { _id: 0, 4: 1 } })
        .toArray()) || [];
    return _.chain(docs).map(4).uniq().compact().value();
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
  let flames_ob = await zedf.race_flames(rid);
  flames_ob = flames_ob?.rpi;
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
  return ret;
};

const zed_push_races_to_mongo = async (races) => {
  if (_.isEmpty(races)) return;
  let horses_ar = [];
  let mongo_push = [];
  for (let r in races) {
    for (let h in races[r]) {
      horses_ar.push({
        hid: parseInt(h),
        rid: r,
        date: races[r][h]["2"],
        tc: races[r][h]["16"],
      });
      mongo_push.push({
        updateOne: {
          filter: { 4: races[r][h]["4"], 6: races[r][h]["6"] },
          update: { $set: { ...races[r][h] } },
          upsert: true,
        },
      });
    }
  }
  if (push_race_horses_on) {
    console.log("race_horses.len:", race_horses.length);
    // console.table(horses_ar)
    await race_horses.push_ar(horses_ar);
  }
  if (!_.isEmpty(mongo_push))
    await zed_ch.db.collection("zed").bulkWrite(mongo_push);
};

const zed_races_gql_runner_inner = async (
  from = null,
  to = null,
  race_conf = { check_exists: false }
) => {
  from = new Date(from).toISOString();
  to = new Date(to).toISOString();

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
    let miss_rids = [];
    let rids = _.keys(races);
    console.log("fetched", rids.length, "race ids");
    if (race_conf.check_exists) {
      let rids_ch = await get_zed_ch_rids_only(from, to);
      let rids_overlap = _.intersection(rids, rids_ch);
      console.log("exists ", rids_overlap.length, "race ids");
      rids = _.filter(rids, (rid) => !rids_overlap.includes(rid));
      console.log("running", rids.length, "race ids");
      races = _.chain(races)
        .entries()
        .filter(([rid, r]) => {
          return rids.includes(rid);
        })
        .toPairs()
        .value();
    }

    for (let [rid, r] of _.entries(races)) {
      if (_.isEmpty(r)) {
        console.log("err", rid, "empty race");
        miss_rids.push(rid);
        continue;
      }

      let l = _.values(r).length;
      if (l == 0) {
        console.log("ERROR EMPTY       ", _.values(r)[0][2], rid, `${l}_h`);
        miss_rids.push(rid);
      } else if (l < 12) {
        console.log("ERROR             ", _.values(r)[0][2], rid, `${l}_h`);
        miss_rids.push(rid);
      } else {
        console.log("getting race from:", _.values(r)[0][2], rid, `${l}_h`);
        min_races.push([rid, r]);
      }
    }
    races = _.chain(min_races).compact().fromPairs().value();
    races = await add_times_flames_odds_to_races(races, config);
    await zed_push_races_to_mongo(races);
    console.log("#DONE", _.keys(races).length, "races\n---");

    if (miss_rids.length > 0) {
      console.log("miss races", miss_rids.length);
      let miss_races = await Promise.all(
        miss_rids.map((rid) =>
          zed_races_zrapi_rid_runner(rid).then((d) => [rid, d])
        )
      );
      miss_races.forEach(([rid, d]) => {
        console.log("GOT at", _.values(d)[0][2], "rid:", rid);
      });
      miss_races = _.fromPairs(miss_races);
      await zed_push_races_to_mongo(races);
      console.log("GOT MISS", _.keys(miss_races).length, "races\n");
    }
  } catch (err) {
    console.log("ERROR on zed_race_add_runner", err.message);
  }
};

const zed_races_gql_runner = async (
  from = null,
  to = null,
  race_conf = {
    check_exists: false,
    durr: 2 * 60 * 1000,
    push_race_horses_on: 1,
  }
) => {
  if (race_conf.push_race_horses_on)
    push_race_horses_on = race_conf.push_race_horses_on;
  let offset = race_conf.durr;
  from = new Date(from).toISOString();
  to = new Date(to).toISOString();
  let now = new Date(from).getTime();
  console.log("RANGE=>", from, to);
  // console.log(now);
  while (now < new Date(to).getTime()) {
    try {
      let now_ed = Math.min(now + offset, nano(to));
      console.log(iso(now), "->", iso(now_ed));
      await zed_races_gql_runner_inner(now, now_ed, race_conf);
      now += offset;
    } catch (err) {
      console.log("zed_races_missing", err);
      await delay(3000);
    }
  }
};

const zed_results_data = async (rid) => {
  // let api = `https://racing-api.zed.run/api/v1/races/result/${rid}`;
  // let doc = await fetch_a(api);
  let doc = await zedf.race_results(rid);
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
  // let api = `https://rpi.zed.run/?race_id=${rid}`;
  // let doc = await fetch_a(api);
  let doc = await zedf.race_flames(rid);
  if (_.isEmpty(doc)) return null;
  let { rpi } = doc;
  return rpi;
};
const zed_race_base_data = async (rid) => {
  // let api = `https://racing-api.zed.run/api/v1/races?race_id=${rid}`;
  // let doc = await fetch_a(api);
  let doc = await zedf.race(rid);
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

const zed_races_zrapi_rid_runner = async (rid, conf = { mode: "err" }) => {
  let { mode = "err" } = conf;
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
  ar = _.keyBy(ar, "6");
  return ar;
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

const zed_races_zrapi_runner = async (
  from = null,
  to = null,
  race_conf = {
    check_exists: true,
    durr: 1 * 60 * 60 * 1000,
    cs: 10,
    push_race_horses_on: 1,
  }
) => {
  let offset = race_conf.durr;
  let cs = race_conf?.cs || 10;
  if (race_conf.push_race_horses_on)
    push_race_horses_on = race_conf.push_race_horses_on;
  // console.log({ from, to });
  from = new Date(from).toISOString();
  to = new Date(to).toISOString();
  let now = new Date(from).getTime();
  console.log("RANGE=>", from, to);
  // console.log(now);
  while (now < new Date(to).getTime()) {
    try {
      console.log("----");
      let now_ed = Math.min(now + offset, nano(to));
      console.log(iso(now), "->", iso(now_ed));
      // now += offset;
      // continue;
      let rids_all = await get_zed_rids_only(now, now_ed);
      console.log("total_races: ", rids_all.length);
      let rids_ch = await get_zed_ch_rids_only(now, now_ed);
      console.log("db_races   : ", rids_ch.length);
      let rids = _.difference(rids_all, rids_ch);
      console.log("miss_races : ", rids.length);
      for (let chunk_rids of _.chunk(rids, cs)) {
        console.log("getting", chunk_rids.toString());
        let races = await Promise.all(
          chunk_rids.map((rid) =>
            zed_races_zrapi_rid_runner(rid).then((d) => [rid, d])
          )
        );
        races.forEach(([rid, d]) => {
          console.log("GOT at", _.values(d)[0][2], "rid:", rid);
        });
        let n = races.length;
        races = _.fromPairs(races);
        await zed_push_races_to_mongo(races);
        console.log("pushed", n, "races\n");
      }
      now += offset;
    } catch (err) {
      console.log("zed_races_missing", err);
      await delay(3000);
    }
  }
  console.log("ENDED");
};

const races_base = {
  zed_races_gql_runner,
  zed_races_zrapi_runner,
};
module.exports = races_base;
