const _ = require("lodash");
const { init, zed_db } = require("./index-run");
const { iso } = require("./utils");
const cron = require("node-cron");
const cron_parser = require("cron-parser");
const axios = require("axios");
const zedf = require("./zedf");

const zed_secret_key = process.env.zed_secret_key;
const zed_gql = "https://zed-ql.zed.run/graphql/getRaceResults";
const mt = 60 * 1000;
let offset = 2 * mt;
let tour_start = "2021-11-17T00:00:00Z";
// let tour_start = "2021-11-16T00:00:00Z";

const get_zed_raw_data = async (from, to) => {
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
    return edges;
  } catch (err) {
    console.log("get_zed_raw_data err", err);
    return [];
  }
};
const pos_pts_ob = {
  1: 89,
  2: 55,
  3: 34,
  4: 21,
  5: 13,
  6: 8,
  7: 5,
  8: 3,
  9: 2,
  10: 1,
  11: 1,
  12: 0,
};
const pos_pts = (pos) => {
  return pos_pts_ob[pos];
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
    class: tc,
    fee: entryfee,
    gates,
    length: dist,
    start_time: date,
    final_positions,
    status,
  } = doc;
  if (status !== "finished") return null;
  let hids = _.values(gates);
  if (!date.endsWith("Z")) date += "Z";
  let ob = { rid, tc, hids, entryfee, dist, date, final_positions, status };
  // console.log("base struct", ob);
  return ob;
};
const get_zed_race_from_api = async (rid) => {
  try {
    console.log("get_zed_race_from_api", rid);
    // return null;
    let [
      base,
      //  results,
      // flames
    ] = await Promise.all([
      zed_race_base_data(rid),
      // zed_results_data(rid),
      // zed_flames_data(rid)
    ]);
    // console.log(base);
    if (_.isEmpty(base) || base.status !== "finished") {
      console.log("couldnt get race", rid);
      return null;
    }
    let { tc, final_positions = {}, dist, start_time } = base;
    let horses = _.entries(final_positions).map(([position, hob]) => {
      let { horse_id: hid, name } = hob;
      // console.log(hid, name);
      return { hid, name, position };
    });
    console.log(start_time);
    let ob = {
      rid,
      tc,
      dist,
      start_time,
      horses,
    };
    return ob;
  } catch (err) {
    // console.log(err);
    return null;
  }
};
const tour_race_not_done = async (rids) => {
  let doc = await zed_db
    .collection("tournament")
    .findOne({ id: "tour_base", rids: { $in: rids } }, { _id: 0, rids: 1 });
  let ar = doc?.rids || [];
  let not_done = _.difference(rids, ar);
  return not_done || [];
};

const struct_horse = (horse) => {
  // console.log(horse);
  // horse = horse.node;
  const { horseId: hid, name, finalPosition: position } = horse;
  // console.log(hid, name);
  return { hid, name, position };
};
const struct_race = (race) => {
  try {
    // console.log(race);
    const { node } = race;
    const {
      class: tc,
      fee,
      length: dist,
      raceId: rid,
      startTime: start_time,
    } = node;

    let { horses = [] } = node;
    horses = horses.map(struct_horse);
    let ob = {
      tc,
      fee,
      dist,
      rid,
      start_time,
      horses,
    };
    return ob;
  } catch (err) {
    console.log("err  at struct_race", err);
    return null;
  }
};

const dists = ["all", 1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600];

const def_horse_ob = ({ hid, name }) => {
  return {
    hid: parseInt(hid),
    name,
    stats: _.chain(dists)
      .map((dist) => [
        dist,
        {
          count: 0,
          avg_pts: 0,
          tot_pts: 0,
        },
      ])
      .fromPairs()
      .value(),
    races: _.chain(dists)
      .map((dist) => [dist, []])
      .fromPairs()
      .value(),
  };
};

const tour_race_horse_update = async (ob) => {
  let { hid, name, position, rid, dist } = ob;
  hid = parseInt(hid);
  let pts = pos_pts(position);
  console.log(`${dist}M`, rid, hid, `#${position}=>(${pts})`, name);
  let doc = await zed_db.db
    .collection("tournament")
    .findOne({ hid }, { hid: 1, stats: 1 });
  if (!doc) {
    doc = def_horse_ob({ hid, name });
    await zed_db.db
      .collection("tournament")
      .updateOne({ hid }, { $set: doc }, { upsert: true });
  }
  let { stats } = doc;
  let all_ob = stats["all"];
  let dist_ob = stats[dist];
  console.log(hid, rid, "all", all_ob);
  console.log(hid, rid, dist, dist_ob);

  all_ob.tot_pts += pts;
  dist_ob.tot_pts += pts;
  all_ob.avg_pts = (all_ob.avg_pts * all_ob.count + pts) / (all_ob.count + 1);
  dist_ob.avg_pts =
    (dist_ob.avg_pts * dist_ob.count + pts) / (dist_ob.count + 1);
  all_ob.count += 1;
  dist_ob.count += 1;
  const update_ob = {
    $set: {
      [`stats.all`]: all_ob,
      [`stats.${dist}`]: dist_ob,
    },
    $addToSet: {
      [`races.all`]: { rid, dist, position, pts },
      [`races.${dist}`]: { rid, dist, position, pts },
    },
  };
  console.log(hid, rid, "all", all_ob);
  console.log(hid, rid, dist, dist_ob);
  await zed_db.db.collection("tournament").updateOne({ hid }, update_ob);
};
const set_tour_races_done = async (rids) => {
  await zed_db.db.collection("tournament").updateOne(
    { id: "tour_base" },
    {
      $set: { id: "tour_base" },
      $addToSet: { rids: { $each: rids } },
    },
    { upsert: true }
  );
};
const zed_tour_fn = async ({ from = null, to = null } = {}) => {
  if (!from) from = Date.now() - offset;
  if (!to) to = Date.now();
  console.log("----\nzed_tour_fn", iso(from), "->", iso(to));
  let raw = await get_zed_raw_data(from, to);
  // raw = raw.slice(0, 1);
  let rids = _.chain(raw)
    .map((i) => i?.node?.raceId)
    .compact()
    .value();
  console.log("found   :", rids.length, raw.length);
  let not_done = await tour_race_not_done(rids);
  console.log("not_done:", not_done.length);
  for (let race of raw) {
    try {
      let { horses, raceId: rid } = race.node;
      if (!not_done.includes(rid)) {
        console.log("ALREADY_DONE", rid, "\n");
        continue;
      }
      if (horses.length !== 12) race = await get_zed_race_from_api(rid);
      else race = struct_race(race);
      if (_.isEmpty(race)) {
        console.log("ERROR", rid);
        continue;
      }
      let { tc, dist, status, start_time } = race;
      console.log(`class#${tc}`, `${dist}M`, rid);
      console.log("start_time:", start_time);
      if (start_time < tour_start) {
        console.log("Tournament NOT YET STARTED");
        continue;
      }
      // console.log("got race", rid);
      horses = race.horses;
      for (let h of horses) {
        if (_.isEmpty(h) || !h.position) console.log(h.hid, "EMPTY");
        await tour_race_horse_update({ ...h, rid, dist });
        console.log(h.hid, "done");
      }
      // console.log(horses);
      await set_tour_races_done([rid]);
      console.log("completed race", rid, "\n");
    } catch (err) {
      console.log("err  at zed_tour_fn", err);
    }
  }
};

const zed_tour_cron = async () => {
  await init();
  console.log("## zed_tour_cron");
  let cron_str = "*/1 * * * *";
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("Next run:", c_itvl.next().toISOString(), "\n");
  cron.schedule(cron_str, () => zed_tour_fn({}), { scheduled: true });
};
const zed_tour_missed_cron = async (p, from, to) => {
  await init();
  console.log("## zed_tour_missed_cron");
  if (p == "test") {
    zed_tour_fn({
      from: Date.now() - 30 * mt,
      to: Date.now() - 2 * mt,
    });
    return;
  } else if (p == "manual") {
    console.log("from:", from);
    console.log("to  :", to);
    zed_tour_fn({
      from,
      to,
    });
  } else {
    let cron_str = "*/5 * * * *";
    const c_itvl = cron_parser.parseExpression(cron_str);
    console.log("Next run:", c_itvl.next().toISOString(), "\n");
    cron.schedule(
      cron_str,
      () =>
        zed_tour_fn({
          from: Date.now() - 11 * mt,
          to: Date.now() - 5 * mt,
        }),
      { scheduled: true }
    );
  }
};

const clear_zed_tour = async () => {
  await init();
  await zed_db.db.collection("tournament").deleteMany({});
  console.log("DELETED tournaments collection");
};

const tunnels = {
  tun0: [1000, 1200, 1400],
  tun1: [1600, 1800, 2000],
  tun2: [2200, 2400, 2600],
};

// 1152
const zed_tour_leader_fn = async ({ limit = 1152 } = {}) => {
  await init();
  console.log("...\n#zed_tour_leader_fn", iso(Date.now()));
  let docs_ar = [];
  for (let dist of dists) {
    console.log("getting zed_tour_leader", dist);
    let docs =
      (await zed_db.db
        .collection("tournament")
        .find(
          {
            hid: { $ne: null },
            [`stats.${dist}`]: { $ne: null },
            [`stats.${dist}.count`]: { $ne: null, $gte: 3 },
          },
          {
            projection: {
              hid: 1,
              name: 1,
              _id: 0,
              stats: 1,
              // [`stats.${dist}`]: 1
            },
          }
        )
        .sort({ [`stats.${dist}.avg_pts`]: -1, [`stats.${dist}.count`]: -1 })
        .limit(limit)
        .toArray()) || [];

    docs_ar = [...docs_ar, ...docs];
    docs = docs.map((doc) => {
      let { hid, name } = doc;
      let ob = doc?.stats[dist] || {};
      return { hid, name, ...ob };
    });
    // console.table(docs);
    let id = `tour_leader_${dist}`;
    await zed_db.db
      .collection("tournament")
      .updateOne({ id }, { $set: { id, ar: docs } }, { upsert: true });
    console.log("uploaded docs", docs.length, "to ", id);
  }
  docs_ar = _.uniqBy(docs_ar, (i) => i.hid);
  docs_ar = docs_ar.map((doc) => {
    let { stats, hid, name } = doc;
    for (let [tun, dis] of _.entries(tunnels)) {
      stats[tun] = dis.reduce(
        (ob, d) => {
          let count = ob.count + stats[d].count;
          let tot_pts = ob.tot_pts + stats[d].tot_pts;
          return {
            count,
            tot_pts,
            avg_pts: count == 0 ? 0 : tot_pts / count,
          };
        },
        { count: 0, avg_pts: 0, tot_pts: 0 }
      );
    }
    return { hid, name, stats };
  });
  for (let dist of _.keys(tunnels)) {
    console.log("getting zed_tour_leader", dist);
    docs = docs_ar.map((doc) => {
      let { hid, name } = doc;
      let ob = doc?.stats[dist] || {};
      return { hid, name, ...ob };
    });
    docs = _.orderBy(
      docs,
      (i) => {
        return [_.toNumber(i.avg_points), _.toNumber(i.count)];
      },
      ["desc", "desc"]
    );
    docs = docs.slice(0, limit);
    // console.table(docs);
    let id = `tour_leader_${dist}`;
    await zed_db.db
      .collection("tournament")
      .updateOne({ id }, { $set: { id, ar: docs } }, { upsert: true });
    console.log("uploaded docs", docs.length, "to ", id);
  }
};
const zed_tour_leader_cron = async () => {
  await init();
  console.log("## zed_tour_leader_crom");
  let cron_str = "*/5 * * * *";
  await zed_tour_leader_fn({});
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("Next run:", c_itvl.next().toISOString(), "\n");
  cron.schedule(cron_str, () => zed_tour_leader_cron({}), {
    scheduled: true,
  });
};

module.exports = {
  zed_tour_fn,
  zed_tour_cron,
  zed_tour_missed_cron,
  clear_zed_tour,
  zed_tour_leader_fn,
  zed_tour_leader_cron,
};
