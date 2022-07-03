const _ = require("lodash");
const moment = require("moment");
const { zed_db, zed_ch } = require("../connection/mongo_connect");
const { speed_dist_factor } = require("../utils/cyclic_dependency");
const cyclic_depedency = require("../utils/cyclic_dependency");
const { nano, iso } = require("../utils/utils");
const norm_time_s = require("./norm_time");

const run_race_speed_adj_raw = (race) => {
  race = _.keyBy(race, "8");
  let dist = race[1][1];
  let times = _.map(race, "7");
  let racetime = _.mean(times);

  for (let [p, r] of _.entries(race)) {
    let finishtime = r[7];
    let adjtime = norm_time_s.eval_norm_only(finishtime, racetime, dist);
    let speed_init = ((dist / adjtime) * 60 * 60) / 1000;
    let speed_rat = speed_init * 1.45 * speed_dist_factor[dist];
    r[23] = adjtime;
    r[24] = speed_init;
    r[25] = speed_rat;
    r[26] = racetime;
  }
  // console.table(
  //   _.map(race, (e) => ({ p: e[8], 23: e[23], 24: e[24], 25: e[25] }))
  // );
  return _.values(race);
};

const run_racesdata_speed_adj_raw = (racesData) => {
  for (let [rid, race] of _.entries(racesData)) {
    race = run_race_speed_adj_raw(race);
  }
  return racesData;
};

const run_rid = async (rid) => {
  let ref = zed_ch.db.collection("zed");
  let race = await ref.find({ 4: rid }).toArray();

  // race = cyclic_depedency.struct_race_row_data(race);
  return run_race_speed_adj_raw(race);
};

const spedit_write_bulk = async (race) => {
  let ref = zed_ch.db.collection("zed");
  // console.table(race);
  let bulk = race.map((e) => ({
    updateOne: {
      filter: { 4: e["4"], 6: e["6"] },
      update: { $set: e },
      upsert: true,
    },
  }));
  console.log("wrote", bulk.length);
  await ref.bulkWrite(bulk);
};

const update_dur = async (from, to) => {
  from = iso(from);
  to = iso(to);
  console.log(from, to);
  let now = nano(from);
  let off = 1 * 60 * 60 * 1000;
  let ref = zed_ch.db.collection("zed");
  do {
    let now_ed = Math.min(now + off, nano(to));
    let raws = await ref
      .find({ 2: { $gte: iso(now), $lte: iso(now_ed) } })
      .toArray();
    raws = _.groupBy(raws, "4");

    console.log(iso(now), iso(now_ed), _.keys(raws).length, "races");

    let upd = [];
    for (let [rid, race] of _.entries(raws)) {
      // console.log(rid);
      race = run_race_speed_adj_raw(race);
      race = race.map((e) => ({
        4: e[4],
        6: e[6],
        23: e[23],
        24: e[24],
        25: e[25],
      }));
      upd.push(race);
    }
    upd = _.flatten(upd);
    await spedit_write_bulk(upd);

    now = now_ed + 1;
  } while (now < nano(to));
};

const test = async () => {
  console.log("test");
  let rid = "6rGVigMv";
  let ob = await run_rid(rid);
  console.table(ob);
};

const main_runner = async () => {
  console.log("race speed adj");
  let [n, f, a1, a2, a3, a4, a5] = process.argv;
  if (a2 == "test") await test();
  if (a2 == "update") await update_dur(a3, a4);
};
const race_speed_adj = {
  main_runner,
  run_race_speed_adj_raw,
  run_racesdata_speed_adj_raw,
  update_dur,
};
module.exports = { race_speed_adj };
