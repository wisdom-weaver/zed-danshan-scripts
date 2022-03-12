const cron = require("node-cron");
const cron_parser = require("cron-parser");
const moment = require("moment");
const { iso, get_N, nano } = require("../utils/utils");
const { get } = require("lodash");
const cyclic_depedency = require("../utils/cyclic_dependency");
const { zed_db, zed_ch } = require("../connection/mongo_connect");
const _ = require("lodash");
const zedf = require("../utils/zedf");

const coll = "gap4";
const cron_conf = { scheduled: true };
const mint = 60 * 1000;
const dur_offset = 1 * 60 * mint;
const def_cs = 15;
let t = 0;

const update_horse_gap = async ({ hid, gap, raceid, date }, p = 1) => {
  hid = parseInt(hid);
  gap = get_N(gap, undefined);

  if (gap && date < "2021-08-30") gap = gap / 2;

  if (p) console.log(`${raceid}:`, { hid, gap });
  if (!hid || gap === undefined) return null;
  let doc = await zed_db.db
    .collection(coll)
    .findOne({ hid }, { projection: { hid: 1, gap: 1 } });
  let gap_ext = doc?.gap || undefined;
  if (gap_ext === undefined || gap_ext < gap) {
    await zed_db.db
      .collection(coll)
      .updateOne({ hid }, { $set: { gap, date } }, { upsert: true });
  }
};

const run_race = async (race = []) => {
  try {
    if (_.isEmpty(race)) return null;
    race = cyclic_depedency.struct_race_row_data(race);
    race = _.uniqBy(race, (e) => `${e.raceid}-${e.hid}`);
    race = race.map((ea) => {
      let { place, flame } = ea;
      place = parseInt(place);
      flame = parseInt(flame);
      return { ...ea, place, flame };
    });
    race = _.keyBy(race, "place");

    let raceid = race[1]?.raceid;
    let dist = race[1]?.distance;
    let date = race[1]?.date;
    dist = parseInt(dist);
    // console.log({ raceid, dist });

    if (race[1] && race[2]) {
      let gap_win = Math.abs(race[1].finishtime - race[2].finishtime);
      gap_win = (gap_win * 1000) / dist;
      if ([0, 1].includes(race[1].flame))
        await update_horse_gap({
          hid: race[1].hid,
          gap: gap_win,
          raceid,
          date,
        });
    }
    if (race[1] && race[2]) {
      let gap_lose = Math.abs(race[11].finishtime - race[12].finishtime);
      gap_lose = (gap_lose * 1000) / dist;
      if ([1].includes(race[12].flame))
        await update_horse_gap({
          hid: race[12].hid,
          gap: gap_lose,
          raceid,
          date,
        });
    }
  } catch (err) {
    let r = race && race[0];
    r = r && r[4];
    console.log(`err at ${r}`, err.message);
  }
};
const run_raw_races = async (races_ar) => {
  races_ar = _.groupBy(races_ar, "4");
  console.log(`GAP:: got ${_.keys(races_ar).length} races`);
  for (let chunk of _.chunk(_.entries(races_ar), def_cs)) {
    let chunk_races = _.map(chunk, 1);
    await Promise.all(chunk_races.map(run_race));
  }
  // console.log("GAP:: DONE");
};

const run_rid = async (rid) => {
  if (!rid) return;
  const race = await zed_ch.db
    .collection("zed")
    .find(
      { 4: rid },
      { projection: { 1: 1, 2: 1, 4: 1, 6: 1, 8: 1, 13: 1, 7: 1 } }
    )
    .toArray();
  await run_race(race);
};

const run_1dur = async (st, ed) => {
  st = iso(st);
  ed = iso(ed);
  let races = await zed_ch.db
    .collection("zed")
    .find(
      { 2: { $gte: st, $lte: ed } },
      { projection: { 1: 1, 2: 1, 4: 1, 6: 1, 8: 1, 13: 1, 7: 1 } }
    )
    .toArray();
  await run_raw_races(races);
};

const run_dur = async (from, to) => {
  from = iso(from);
  to = iso(to);
  console.log("GAP=====>::", from, "->", to);
  for (let now = nano(from); now <= nano(to); ) {
    let now_ed = Math.min(now + dur_offset, nano(to));
    let now_i = iso(now);
    let now_ed_i = iso(now_ed);
    console.log("GAP::", now_i, "->", now_ed_i);
    await run_1dur(now, now_ed);
    now = now_ed + 1;
  }
  console.log("GAP:: DONE run_dur");
};
const manual = async (rids) => {
  for (let rid of rids) {
    // console.log(rid);
    await run_rid(rid);
  }
};
const run_hid = async (hid) => {
  hid = parseFloat(hid);
  let rids =
    (await zed_ch.db
      .collection("zed")
      .find({ 6: hid }, { projection: { _id: 0, 4: 1 } })
      .toArray()) || [];
  rids = _.uniq(_.map(rids, "4"));
  for (let rid of rids) {
    await run_rid(rid);
  }
};
const run_hids = async (hids) => {
  console.log(hids);
  for (let hid of hids) {
    await run_hid(hid);
    console.log(hid);
  }
};

const fix1 = async (hid) => {
  let aft = await zed_db.db.collection("gap6").findOne({ hid });
  if (aft?.gap) {
    await zed_db.db
      .collection(coll)
      .updateOne(
        { hid },
        { $set: { gap: aft.gap, date: aft.date } },
        { upsert: true }
      );
    return;
  }
  let bef = await zed_db.db.collection("gap5").findOne({ hid });
  if (bef?.gap) {
    await zed_db.db
      .collection(coll)
      .updateOne(
        { hid },
        { $set: { gap: bef.gap / 2, date: bef.date } },
        { upsert: true }
      );
    return;
  }
};

const fix2 = async (hid) => {
  let g4 = (await zed_db.db.collection("gap4").findOne({ hid })) || {};
  let bb =
    (await zed_db.db
      .collection("rating_blood3")
      .findOne({ hid }, { projection: { hid: 1, races_n: 1 } })) || {};
  let ngap = null;
  let races_n = bb?.races_n ?? null;
  if (races_n == null) {
    let zedd = await zedf.horse(hid);
    races_n = zedd.number_of_races;
    await zed_db.db
      .collection("rating_blood3")
      .updateOne({ hid }, { $set: { races_n } });
  }

  if (races_n == undefined) return console.log("races undefined", hid);
  if (races_n == 0) ngap = null;
  else {
    if (g4.gap) return;
    else ngap = ((hid % 100) + 1) * 0.001;
  }
  // console.log("fix", { hid, ngap });
  await zed_db.db
    .collection("gap4")
    .updateOne({ hid }, { $set: { gap: ngap } }, { upsert: true });
};

const fix = async () => {
  // let hids = await cyclic_depedency.get_all_hids();
  // // let hids = [11084];
  // for (let chu of _.chunk(hids, 100)) {
  //   await Promise.all(chu.map(fix2));
  //   let [a, b] = [chu[0], chu[chu.length - 1]];
  //   console.log(a, "->", b);
  // }
  const ob = await zed_db.db
    .collection("gap4")
    .find({
      gap: { $gte: 1.2, $lte: 1.3 },
      hid: { $gte: 200000 },
    })
    .toArray();
  let ob2 = [];
  for (let { hid, gap, dist, date } of ob) {
    // console.log({ hid, gap, dist, date });
    let rdoc = await zed_ch.db.collection("zed").findOne({
      6: hid,
      2: { $regex: date.slice(0, 19) },
    });
    if (!rdoc) continue;
    // console.log(rdoc);
    let race_id = rdoc[4];
    let rdocs = await zed_ch.db
      .collection("zed")
      .find({ 4: race_id }, { projection: { _id: 0, 7: 1 } })
      .toArray();
    let avg_race_time = _.chain(rdocs).map("7").mean().value();
    ob2.push({
      hid,
      rng: gap,
      gap_sec: (gap * rdoc[1]) / 1000,
      date,
      dist: rdoc[1],
      race_id,
      avg_race_time,
      hrace_time: rdoc[7],
    });
  }
  console.table(ob2);
};

const gap = {
  run_race,
  run_raw_races,
  run_dur,
  fix: fix2,
  manual,
  run_hids,
};
module.exports = gap;
