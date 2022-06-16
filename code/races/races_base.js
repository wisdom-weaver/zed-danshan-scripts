const axios = require("axios");
const _ = require("lodash");
const { init, zed_ch, zed_db } = require("../connection/mongo_connect");
const { delay, iso, nano, get_fee_tag, getv } = require("../utils/utils");
const {
  get_adjusted_finish_times,
} = require("./zed_races_adjusted_finish_time");
const { get_fee_cat_on, get_entryfee_usd } = require("../utils/base");
const { get_sims_zed_odds } = require("./sims");
const zedf = require("../utils/zedf");
const race_horses = require("./race_horses");
const max_gap = require("../dan/max_gap");
const gap = require("../v3/gaps");
const compiler = require("../dan/compiler/compiler_dp");
const utils = require("../utils/utils");
const moment = require("moment");
const { race_speed_adj } = require("./race_speed_adj");

const zed_gql = "https://zed-ql.zed.run/graphql/getRaceResults";
const zed_secret_key = process.env.zed_secret_key;
let test_mode = 0;

const mt = 60 * 1000;
const hr = 1000 * 60 * 60;
const day_diff = 1000 * 60 * 60 * 24 * 1;
const g_h = 72;

let push_race_horses_on = 0;

const def_cs = 15;

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

const get_hratings = async (hids) => {
  let hsdoc = await zedf.horses(hids);
  return _.chain(hsdoc)
    .map((e) => [e.horse_id, e.rating])
    .fromPairs()
    .value();
};

const wrap_rating = async ([race_id, raceData]) => {
  let hids = _.keys(
    raceData,
    _.map((e) => parseInt(e))
  );
  let now = moment();
  let date = getv(raceData, "${hids[0]}.2") ?? null;
  if (date == null) return;
  let start = moment(date);
  let diff = now.diff(start, "seconds");
  // console.log("date_diff", diff);
  if (diff > 500) return;

  hsdoc = await get_hratings(hids);

  _.entries(hsdoc).map(([horse_id, rating]) => {
    raceData[horse_id][22] = rating;
  });
  // console.table(_.values(raceData));
};

const wrap_rating_for_races = async (racesData) => {
  let proms = [];
  for (let [rid, data] of _.entries(racesData)) {
    proms.push(wrap_rating([rid, data]));
  }
  await Promise.all(proms);
};

const get_zed_raw_data = async (from, to, cursor, lim = 100) => {
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

        prizePool {
          first
          second
          third
        }

        horses {
          horseId
          finishTime
          finalPosition
          name
          gate
          rating
          ownerAddress
          class

        }
      }
    }
    pageInfo: page_info {
      startCursor: start_cursor
      endCursor: end_cursor
      hasNextPage: has_next_page
      hasPreviousPage: has_previous_page
      __typename
    }
    __typename
  }
}`,
      variables: {
        first: lim,
        after: cursor,
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
    let pageInfo = result?.data?.data?.getRaceResults?.pageInfo || {};
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
      let race_name = node.name;
      let pool = {
        1: parseFloat(node.prizePool.first) / 1e18,
        2: parseFloat(node.prizePool.second) / 1e18,
        3: parseFloat(node.prizePool.third) / 1e18,
      };

      racesData[node_raceId] = {};
      for (let horseIndex in horses) {
        let horse = horses[horseIndex];
        let horses_horseId = horse.horseId;
        let horses_finishTime = horse.finishTime;
        let horses_finalPosition = horse.finalPosition;
        let horses_name = horse.name;
        let horses_gate = horse.gate;
        let horses_class = horse.class;
        let fee_usd = get_entryfee_usd({ fee: node_fee, date: node_startTime });
        let fee_tag = get_fee_tag(fee_usd);
        let prize = pool[horses_finalPosition] || 0;
        let prize_usd = get_entryfee_usd({ fee: prize, date: node_startTime });

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
          17: race_name,
          18: fee_usd,
          19: fee_tag,
          20: prize,
          21: prize_usd,
        };
      }
    }

    await wrap_rating_for_races(racesData);
    await race_speed_adj.run_racesdata_speed_adj_raw(racesData);
    if (false)
      for (let [rid, raceData] of _.entries(racesData))
        console.table(_.values(raceData));
    return { racesData, pageInfo };
  } catch (err) {
    console.log("err", err);
    return [];
  }
};
const get_zed_rids_only_inner = async (from, to, cursor, lim = 100) => {
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
    pageInfo: page_info {
      startCursor: start_cursor
      endCursor: end_cursor
      hasNextPage: has_next_page
      hasPreviousPage: has_previous_page
      __typename
    }
    __typename
  }
}`,
      variables: {
        first: lim,
        after: cursor,
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
    let pageInfo = result?.data?.data?.getRaceResults?.pageInfo || {};

    let racesData = [];
    racesData = _.map(edges, (e) => e.node.raceId);
    return { rids: racesData, pageInfo };
  } catch (err) {
    console.log("err", err);
    return [];
  }
};

const get_zed_rids_only = async (from, to) => {
  let rids = [],
    cursor,
    pinf;
  from = iso(from);
  to = iso(to);
  do {
    let resp = await get_zed_rids_only_inner(from, to, cursor, 100);
    pinf = resp.pageInfo;
    cursor = pinf.endCursor;
    let ccrids = resp.rids;

    // console.log({ cursor }, pinf.hasNextPage, ccrids?.length);
    rids.push(ccrids);
  } while (pinf.hasNextPage);
  rids = _.flatten(rids);
  rids = _.chain(rids).uniq().compact().value();
  // console.log("got", rids.length);
  return rids;
};

const get_zed_ch_rids_only = get_zed_rids_only;

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

  // odds_ob = await get_sims_zed_odds(rid, "raw_race", raw_race);

  // if (!_.isEmpty(config) && config.g_odds_zero == true) {
  //   if (thisclass == 0) odds_ob = {};
  //   else odds_ob = await get_sims_zed_odds(rid, "raw_race", raw_race);
  // } else odds_ob = await get_sims_zed_odds(rid, "raw_race", raw_race);
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
  console.log("add_times_flames_odds_to_races");
  // let ret = {};
  if (_.isEmpty(raw_data)) return {};
  let ret = [];
  let cs = def_cs;
  let i = 0;
  for (let data_chunk of _.chunk(_.entries(raw_data), cs)) {
    try {
      console.log("times", i);
      let this_chunk = await Promise.all(
        data_chunk.map(([rid, race]) =>
          add_times_flames_odds_to_1race([rid, race], config)
        )
      );
      i += this_chunk.length;
      ret = [...ret, ...this_chunk];
    } catch (err) {
      console.log(err);
    }
  }
  ret = _.chain(ret)
    .filter((i) => i[1] !== null)
    .fromPairs()
    .value();
  return ret;
};

const zed_push_races_to_mongo = async (races) => {
  if (_.isEmpty(races)) return [];
  let horses_ar = [];
  let mongo_push = [];
  let races_ar = [];
  for (let r in races) {
    races_ar = [...races_ar, ..._.values(races[r])];
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
  let hids = _.map(horses_ar, "hid");
  // if (push_race_horses_on) {
  console.log("race_horses.len:", horses_ar.length);
  await race_horses.push_ar(horses_ar);
  // }
  if (!_.isEmpty(mongo_push)) {
    await zed_ch.db.collection("zed").bulkWrite(mongo_push);
  }
  console.log(hids.length, "pushed docs");
  if (!_.isEmpty(races_ar)) {
    // await max_gap.raw_race_runner(races_ar);
    await gap.run_raw_races(races_ar);
  }
  if (!_.isEmpty(hids)) {
    // await compiler.run_hs(hids);
  }
  return _.keys(races);
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
    let races = [],
      cursor,
      pinf;
    do {
      let resp = await get_zed_raw_data(from, to, cursor, 100);
      pinf = resp.pageInfo;
      cursor = pinf.endCursor;
      let ccraces = resp.racesData;

      console.log({ cursor }, pinf.hasNextPage, _.keys(ccraces)?.length);
      races = { ...races, ...ccraces };
    } while (pinf.hasNextPage);
    console.log("got", parseInt(_.keys(races).length));

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
        .fromPairs()
        .value();
    }

    console.log(rids?.join(", "));
    // console.log(_.keys(races))

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
    name: race_name,
    prize,
  } = doc;
  if (status !== "finished") return null;
  let hids = _.values(gates);
  if (!date.endsWith("Z")) date += "Z";
  let ob = { rid, thisclass, hids, entryfee, distance, date, race_name, prize };
  // console.log("base struct", ob);
  return ob;
};

const check_exists_rids = async (rids) => {
  let docs = await zed_ch.db
    .collection("zed")
    .find({ 4: { $in: rids } }, { projection: { 4: 1, 6: 1, _id: 0 } })
    .toArray();
  docs = _.groupBy(docs, 4);
  let rids_exists = _.chain(docs)
    .entries()
    .filter(([rid, r]) => {
      // console.log(rid, r.length);
      if (_.isEmpty(r)) return false;
      return r?.length >= 12;
    })
    .map(0)
    .value();
  return rids_exists;
};
const check_missing_rids = async (rids) => {
  let exists = await check_exists_rids(rids);
  let miss = _.difference(rids, exists);
  return miss;
};

const zed_races_zrapi_rid_runner = async (
  rid,
  conf = { mode: "err", check_exists: 0 }
) => {
  // console.log("zed_races_zrapi_rid_runner", rid);
  let { mode = "err" } = conf;
  let [base, results, flames] = await Promise.all([
    zed_race_base_data(rid),
    zed_results_data(rid),
    zed_flames_data(rid),
  ]);
  if (_.isEmpty(base) || _.isEmpty(results) || _.isEmpty(flames)) {
    // console.log("couldnt get race", rid);
    return [];
  }
  let { hids, thisclass, entryfee, distance, date, race_name } = base;
  let fee_cat = get_fee_cat_on({ date, fee: entryfee });
  let ar = hids.map((hid) => {
    return {
      hid,
      ...results[hid],
      flame: flames[hid],
    };
  });
  // console.log("base", base);
  
  let pool = {
    1: parseFloat(getv(base, "prize.first") ?? 0) / 1e18,
    2: parseFloat(getv(base, "prize.second") ?? 0) / 1e18,
    3: parseFloat(getv(base, "prize.third") ?? 0) / 1e18,
  };

  let now = moment();
  let start = moment(date);
  let diff = now.diff(start, "seconds");

  let hsdoc = {};
  let doh = diff < 500;
  if (doh) {
    hsdoc = await zedf.horses(hids);
    hsdoc = _.chain(hsdoc)
      .map((e) => [e.horse_id, { hclass: e.class, rating: e.rating }])
      .fromPairs()
      .value();
    // console.log(hsdoc)
  }

  let fee_usd = get_entryfee_usd({ fee: entryfee, date: date });
  let fee_tag = get_fee_tag(fee_usd);
  ar = ar.map((i) => {
    let prize = pool[i.place] || 0;
    let prize_usd = get_entryfee_usd({ fee: prize, date });
    return {
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
      ...(doh ? { 16: hsdoc[i.hid].hclass } : {}),
      17: race_name,
      18: fee_usd,
      19: fee_tag,
      20: prize,
      21: prize_usd,
      ...(doh ? { 22: hsdoc[i.hid].rating } : {}),
    };
  });
  let adj_ob = await get_adjusted_finish_times(rid, "raw_data", ar);
  // console.log(rid, adj_ob);

  ar = ar.map((i) => ({ ...i, 15: adj_ob[i[6]] }));
  if (mode == "g" || (mode == "err" && thisclass !== 0)) {
    // let odds_ob = await get_sims_zed_odds(rid, "raw_race", ar);
    // ar = ar.map((i) => ({ ...i, 11: odds_ob[i[6]] }));
    ar = ar.map((i) => ({ ...i, 11: null }));
    // console.log(mode, thisclass, odds_ob);
  }
  // console.log(ar)
  ar = race_speed_adj.run_race_speed_adj_raw(ar);
  ar = _.keyBy(ar, "6");
  // console.table(ar)
  // console.log(rid);
  // console.table(ar);
  return ar;
};

const zed_race_odds_struct_mongodb = async (rid) => {
  // let ob = await get_sims_zed_odds(rid);
  // ob = _.entries(ob).map(([hid, odds]) => [
  //   hid,
  //   { 4: rid, 6: parseFloat(hid), 11: odds },
  // ]);
  // ob = _.fromPairs(ob);
  // return ob;
};

const zed_races_zrapi_runner = async (
  from = null,
  to = null,
  race_conf = {
    check_exists: true,
    durr: 1 * 60 * 60 * 1000,
    cs: def_cs,
    push_race_horses_on: 1,
  }
) => {
  let offset = race_conf.durr;
  let cs = race_conf?.cs || def_cs;
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
      let rids = [];
      if (race_conf?.check_exists) {
        let rids_ch = await get_zed_ch_rids_only(now, now_ed);
        console.log("db_races   : ", rids_ch.length);
        rids = _.difference(rids_all, rids_ch);
        console.log("miss_races : ", rids.length);
      } else rids = rids_all;
      for (let chunk_rids of _.chunk(rids, cs)) {
        console.log("getting", chunk_rids.toString());
        let races = await Promise.all(
          chunk_rids.map((rid) =>
            zed_races_zrapi_rid_runner(rid).then((d) => [rid, d])
          )
        );
        races = _.compact(races);
        races = races.filter(([rid, d]) => {
          let n = _.values(d)?.length || 0;
          if (n == 0) console.log("EMPTY", "rid:", rid);
          else if (n == 12)
            console.log("GOT", _.values(d)[0][2], "rid:", rid, n);
          else console.log("ERR", _.values(d)[0][2], "rid:", rid, n);
          return n == 12;
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
const zed_race_run_rids = async (rids, cs = def_cs) => {
  try {
    let all_pushed_n = [];
    for (let chunk_rids of _.chunk(rids, cs)) {
      console.log("getting", chunk_rids.toString());
      let races = await Promise.all(
        chunk_rids.map((rid) =>
          zed_races_zrapi_rid_runner(rid).then((d) => [rid, d])
        )
      );
      races = _.compact(races);
      races = races.filter(([rid, d]) => {
        let n = _.values(d)?.length || 0;
        if (n == 0) console.log("EMPTY", "rid:", rid);
        else if (n == 12) console.log("GOT", _.values(d)[0][2], "rid:", rid, n);
        else console.log("ERR", _.values(d)[0][2], "rid:", rid, n);
        return n == 12;
      });
      let n = races.length;
      console.log("NNN", n);
      races = _.fromPairs(races);
      let pushed_n = await zed_push_races_to_mongo(races);
      // await zed_db.db.collection("sraces").deleteMany({ rid: { $in: pushed_n } });
      console.log("pushed", n, "races\n");
      all_pushed_n = [...all_pushed_n, ...pushed_n];
    }
    return all_pushed_n;
  } catch (err) {
    console.log("err", err);
    return 0;
  }
};

const races_base = {
  zed_races_gql_runner,
  zed_races_zrapi_runner,
  zed_race_base_data,
  zed_race_run_rids,
  check_exists_rids,
  check_missing_rids,
  get_zed_rids_only,
  zed_races_zrapi_rid_runner,
  get_zed_raw_data,
};
module.exports = races_base;
