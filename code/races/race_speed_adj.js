const _ = require("lodash");
const moment = require("moment");
const { zed_db, zed_ch } = require("../connection/mongo_connect");
const cyclic_depedency = require("../utils/cyclic_dependency");
const { nano, iso } = require("../utils/utils");

const dtab = {
  1000: { t6: 57.57086259, t7: 57.63568977, t67mean: 57.63568977, fact: 1 },
  1200: {
    t6: 71.24295878,
    t7: 71.31171949,
    t67mean: 71.31171949,
    fact: 1.03141812,
  },
  1400: {
    t6: 83.47609738,
    t7: 83.54868612,
    t67mean: 83.54868612,
    fact: 1.03585102,
  },
  1600: {
    t6: 95.61032059,
    t7: 95.69615259,
    t67mean: 95.69615259,
    fact: 1.03832922,
  },
  1800: {
    t6: 107.7237292,
    t7: 107.8215945,
    t67mean: 107.8215945,
    fact: 1.03984722,
  },
  2000: {
    t6: 119.7604036,
    t7: 119.872628,
    t67mean: 119.872628,
    fact: 1.04114768,
  },
  2200: {
    t6: 131.9572636,
    t7: 132.0844164,
    t67mean: 132.0844164,
    fact: 1.04225978,
  },
  2400: {
    t6: 144.0485202,
    t7: 144.2163933,
    t67mean: 144.2163933,
    fact: 1.04259176,
  },
  2600: {
    t6: 156.2421209,
    t7: 156.4615938,
    t67mean: 156.4615938,
    fact: 1.04369422,
  },
};

const run_race_speed_adj_raw = (race) => {
  race = _.keyBy(race, "8");
  let dist = race[1][1];
  let t6 = race[6][7];
  let t7 = race[7][7];
  const t67mean = _.mean([t6, t7]);
  const tmean = dtab[dist].t67mean;
  let tadj = tmean - t67mean;

  for (let [p, r] of _.entries(race)) {
    let th = r[7];
    let timeadj = th + tadj;
    let speed_init = ((dist / timeadj) * 60 * 60) / 1000;
    let speed_rat = speed_init * 1.45 * dtab[dist].fact;
    r[23] = timeadj;
    r[24] = speed_init;
    r[25] = speed_rat;
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
      console.log(rid);
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
