const cron = require("node-cron");
const cron_parser = require("cron-parser");
const moment = require("moment");
const { iso, get_N, nano } = require("../utils/utils");
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

const update_horse_gap = async ({ hid, gap, raceid, date }, p = 0) => {
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
      { projection: { 1: 1, 2: 1, 4: 1, 5: 1, 6: 1, 8: 1, 13: 1, 7: 1 } }
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
      { projection: { 1: 1, 2: 1, 4: 1, 5: 1, 6: 1, 8: 1, 13: 1, 7: 1 } }
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

const fix = async () => {
  let hids = await cyclic_depedency.get_all_hids();
  // hids = hids.slice(0, 5000);
  let null_hids = [];
  for (let chu of _.chunk(hids, 2000)) {
    console.log("get null_hids:", chu[0], chu[chu.length - 1]);
    let nhids = await zed_db.db
      .collection(coll)
      .find(
        { gap: { $in: [null, NaN, 0] }, hid: { $in: chu } },
        { projection: { hid: 1, _id: 0 } }
      )
      .toArray();
    nhids = _.map(nhids, "hid") || [];
    let nhids2 = await zed_db.db
      .collection("rating_blood3")
      .find(
        { races_n: { $gte: 35 }, hid: { $in: nhids } },
        { projection: { hid: 1, _id: 0 } }
      )
      .toArray();
    nhids2 = _.map(nhids2, "hid");
    null_hids.push(nhids2);
  }
  null_hids = _.flatten(null_hids);
  console.log("## null_hids:", null_hids.length);

  let rids = [];
  for (let chu of _.chunk(null_hids, 200)) {
    console.log("get rids:", chu[0], chu[chu.length - 1]);
    let nrids = await zed_ch.db
      .collection("zed")
      .find({ 6: { $in: chu } }, { projection: { 4: 1, _id: 0 } })
      .toArray();
    nrids = _.map(nrids, "4") || [];
    rids.push(nrids);
  }
  rids = _.chain(rids).flatten().uniq().compact().value();
  console.log("## null_rids:", rids.length);
  // console.log(rids.join(""));

  let i = 0;
  for (let chu of _.chunk(rids, 500)) {
    await Promise.all(chu.map(run_rid));
    i += chu.length;
    console.log("races complete:", i);
  }
};

const get = async (hid) => {
  hid = parseInt(hid);
  if (!hid || _.isNaN(hid)) return null;
  let doc = await zed_db.db
    .collection(coll)
    .findOne({ hid }, { projection: { gap: 1 } });
  return doc?.gap || null;
};

const gap = {
  run_race,
  run_raw_races,
  run_dur,
  fix,
  manual,
  run_hids,
  get,
};
module.exports = gap;
