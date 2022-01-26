const _ = require("lodash");
const moment = require("moment");
const cron = require("node-cron");
const cron_parser = require("cron-parser");
const { zed_ch, zed_db } = require("../connection/mongo_connect");
const cyclic_depedency = require("../utils/cyclic_dependency");
const { nano, iso } = require("../utils/utils");
const utils = require("../utils/utils");
const zedf = require("../utils/zedf");
const { zed_race_base_data } = require("../races/races_base");

const coll = "tourneyr01";
const coll2 = "tourneyr01_leader";
const coll3 = "tourneyr01_sraces";
const dur = 2.2 * 60 * 1000;

let t_st_date = "2022-01-19T00:00:00.000Z";
let t_ed_date = "2022-01-26T00:00:00.000Z";
let r2_st = t_ed_date;
let r2_ed = "2022-01-26T22:30:00.000Z";

let stable_ob = [];
let all_hids = [];
let unpaid_hids = [];

const calc_horse_points = async (hid) => {
  hid = parseFloat(hid);
  let races =
    (await zed_ch.db
      .collection("zed")
      .find(
        { 2: { $gte: t_st_date, $lte: t_ed_date }, 6: hid },
        { projection: { 6: 1, 8: 1 } }
      )
      .toArray()) ?? [];
  let poss = _.map(races, (i) => parseFloat(i[8]));
  let pts = poss.reduce((acc, e) => acc + ([1, 2, 3].includes(e) ? 1 : 0), 0);
  let avg = pts / poss.length;
  if (poss.length == 0) avg = 0;
  let traces_n = poss.length;
  let stable_name = get_stable_name(hid);
  let ob = { hid, pts, avg, traces_n, stable_name };
  console.log(`::${hid} #${traces_n}`, { avg, pts });
  // console.log(poss);
  await zed_db.db
    .collection(coll2)
    .updateOne({ hid }, { $set: ob }, { upsert: true });
};

const r2_horse_eval = async (hid) => {
  let races =
    (await zed_ch.db
      .collection("zed")
      .find(
        { 2: { $gte: r2_st, $lte: r2_ed }, 6: hid },
        { projection: { 6: 1, 4: 1, 8: 1, 17: 1 } }
      )
      .toArray()) ?? [];

  let update_ob = {};
  for (let race of races) {
    let { 17: race_name, 8: place, 4: rid } = race;
    if (race_name.includes("A QF")) {
      update_ob.qf = 1;
      update_ob.qf_ob = { rid, place };
    }
    if (race_name.includes("A SF")) {
      update_ob.sf = 1;
      update_ob.sf_ob = { rid, place };
    }
    if (race_name.includes("A Final")) {
      update_ob.f = 1;
      update_ob.f_ob = { rid, place };
    }
  }
  if (!_.isEmpty(update_ob))
    await zed_db.db.collection(coll2).updateOne({ hid }, { $set: update_ob });
};
const r2_tr_sraces_eval = async () => {
  // await init_run();
  let races =
    (await zed_db.db
      .collection(coll3)
      .find(
        {
          hids: { $in: all_hids },
          thisclass: 99,
        },
        { projection: { hids: 1, race_name: 1, race_id: 1, thisclass: 1 } }
      )
      .toArray()) ?? [];
  if (_.isEmpty(races)) {
    console.log("no races");
    return;
  }
  for (let race of races) {
    let { hids, race_name, race_id, thisclass } = race;
    let our_hids = _.intersection(hids, all_hids);
    console.log("got", race_id, `(${thisclass})`, race_name, our_hids);
    if (_.isEmpty(our_hids)) continue;
    for (let hid of our_hids) {
      let update_ob = {};
      let obid = null;
      if (race_name.includes("A QF")) {
        update_ob.qf = 1;
        obid = "qf_ob";
      }
      if (race_name.includes("A SF")) {
        update_ob.sf = 1;
        obid = "sf_ob";
        console.log(hid, update_ob);
      }
      if (race_name.includes("A Final")) {
        update_ob.f = 1;
        obid = "f_ob";
      }
      // console.log(update_ob);
      if (!_.isEmpty(update_ob)) {
        await zed_db.db
          .collection(coll2)
          .updateOne(
            { hid },
            { $set: { ...update_ob, [`${obid}.rid`]: race_id } }
          );
      }
    }
  }
};

let tr_sch_api = `https://racing-api.zed.run/api/v1/races?status=scheduled&class=99`;
// let tr_sch_api = `https://racing-api.zed.run/api/v1/races?status=scheduled`;
const get_scheduled_races = async () => {
  let races = [];
  let n = [];
  let offset = 0;
  do {
    let api = tr_sch_api + `&offset=${offset}`;
    n = await zedf.get(api);
    console.log({ offset, n: n.length });
    if (_.isEmpty(n)) n = [];
    races = [...races, ...n];
    offset += n.length;
  } while (n.length !== 0);
  return races;
};
const struct_race = (doc) => {
  let {
    race_id,
    class: thisclass,
    fee: entryfee,
    gates,
    length: distance,
    start_time: date,
    status,
    name: race_name,
    prize,
  } = doc;
  entryfee = parseFloat(entryfee);
  let hids = _.values(gates);
  return {
    race_id,
    thisclass,
    entryfee,
    gates,
    hids,
    distance,
    date,
    status,
    race_name,
    prize,
  };
};
let prev_flames = 0;
const r2_get_scheduled = async () => {
  console.log("r2_get_scheduled");
  let races = (await get_scheduled_races()) ?? [];
  console.log("#scheduled_races:", races.length);
  if (_.isEmpty(races)) return;
  races = races.map(struct_race);

  let race_ids = _.map(races, "race_id");
  let exists = await zed_db.db
    .collection(coll3)
    .find({ race_id: { $in: race_ids } }, { projection: { race_id: 1 } })
    .toArray();
  exists = _.map(exists, "race_id");
  let eval_raceids = _.difference(race_ids, exists);
  let eval_races = races.filter((r) => eval_raceids.includes(r.race_id));
  if (prev_flames) eval_races = races;
  console.log("new_races", eval_races.length);
  console.log(JSON.stringify(_.map(eval_races, "race_id")));
  if (_.isEmpty(eval_races)) return;
  for (let r of eval_races) {
    let { race_id } = r;
    let flames_doc = await zedf.race_flames(race_id);
    flames_doc = flames_doc.rpi;
    r.flames_doc = flames_doc;
  }
  if (prev_flames) {
    let bulk = eval_races.map((r) => {
      return {
        updateOne: {
          filter: { race_id: r.race_id },
          update: { $set: r },
          upsert: true,
        },
      };
    });
    await zed_db.db.collection(coll3).bulkWrite(bulk);
    console.log("done", bulk.length);
  } else {
    await zed_db.db.collection(coll3).insertMany(eval_races);
    console.log("done", eval_races.length);
  }
};

const run_dur = async ([st, ed]) => {
  await init_run();
  console.log("run_dur", [st, ed]);
  st = iso(st);
  ed = iso(ed);
  let races = await zed_ch.db
    .collection("zed")
    .find(
      { 2: { $gte: st, $lte: ed } },
      { projection: { 4: 1, 6: 1, 8: 1, 5: 1, 2: 1 } }
    )
    .toArray();
  console.log("docs.len", races.length);
  races = _.groupBy(races, 4);
  for (let [rid, race] of _.entries(races)) {
    race = _.sortBy(race, (i) => parseFloat(i[8]));
    let c = race[0][5];
    let hids = _.map(race, 6);
    let top3 = hids.slice(0, 3);
    console.log(rid, `C${c}`, race.length, "top3:", top3);

    let rdate = race[0][2];
    if (rdate >= t_st_date && rdate <= t_ed_date) {
      let to_eval = top3.filter((h) => all_hids.includes(h));
      await Promise.all(to_eval.map((h) => calc_horse_points(h)));
    }
    if (rdate >= r2_st && rdate <= r2_ed) {
      let to_eval = hids.filter((h) => all_hids.includes(h));
      await Promise.all(to_eval.map((h) => r2_horse_eval(h)));
    }
  }
};
const now = async () => {
  let ed = moment().subtract(2, "minutes").toISOString();
  let st = moment(new Date(nano(ed) - dur)).toISOString();
  await run_dur([st, ed]);
};

const get_stable_ob = async () => {
  let ob = await zed_db.db
    .collection(coll)
    .find({ stable_name: { $ne: null } })
    .toArray();
  return ob;
};
const update_hids_list = async () => {
  if (!stable_ob) stable_ob = await get_stable_ob();
  let hids = [];
  let un = [];
  for (let { payments } of stable_ob) {
    for (let p of payments) {
      if (p.status == "paid") {
        hids.push(p?.add_hids ?? []);
      } else {
        un.push(p?.add_hids ?? []);
      }
    }
  }
  all_hids = _.compact(_.flatten(hids));
  all_hids = _.uniq(all_hids);
  unpaid_hids = _.compact(_.flatten(un));
};

const get_stable_name = (hid) => {
  let ob = _.chain(stable_ob).keyBy("stable_name").mapValues("hids").value();
  for (let [stable, hids] of _.entries(ob)) {
    if (hids.includes(hid)) return stable;
  }
  return "na-stable";
};

const update_hid_docs = async () => {
  let ob = { id: "master", hids: all_hids };
  await zed_db.db
    .collection(coll2)
    .updateOne({ id: "master" }, { $set: ob }, { upsert: true });
  let hids_exists = await zed_db.db
    .collection(coll2)
    .find({ hid: { $in: all_hids } }, { projection: { hid: 1, _id: 0 } })
    .toArray();
  hids_exists = _.map(hids_exists, "hid");
  let miss = _.difference(all_hids, hids_exists);
  if (!_.isEmpty(miss)) await Promise.all(miss.map(calc_horse_points));
  if (!_.isEmpty(unpaid_hids))
    await zed_db.db.collection(coll2).deleteMany({ hid: { $in: unpaid_hids } });
};

const init_run = async () => {
  console.log("init");
  stable_ob = await get_stable_ob();
  await update_hids_list();
  console.log("stables:", stable_ob.length);
  console.log("paid  :", all_hids.length);
  console.log("unpaid:", unpaid_hids.length);
  await update_hid_docs();
};

const run_cron = async () => {
  console.log("run_cron");
  let cron_str = "*/2 * * * *";
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("Next run:", c_itvl.next().toISOString(), "\n");
  let runner = now;
  cron.schedule(cron_str, runner, utils.cron_conf);
};

const do_horse = async (hid) => {
  await calc_horse_points(hid);
  await utils.delay(200);
  await r2_horse_eval(hid);
};

const now_h_old = async () => {
  await init_run();
  console.log("now_h:", iso());
  for (let chu of _.chunk(all_hids, 25)) {
    await Promise.all(chu.map(do_horse));
  }
  console.log("now_h:", iso(), "\n----------");
};
const now_h = async () => {
  await init_run();
  console.log("now_h:", iso());
  await r2_tr_sraces_eval();
  for (let chu of _.chunk(all_hids, 25)) {
    await Promise.all(chu.map(r2_horse_eval));
  }
  console.log("now_h:", iso(), "\n----------");
};

const now_scheduled = async () => {
  console.log("now_scheduled", iso());
  await r2_get_scheduled();
  console.log("now_scheduled ended", iso());
};

const run_cron_h = async () => {
  console.log("run_cron_h");
  let cron_str = "*/1 * * * *";
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("Next run:", c_itvl.next().toISOString(), "\n");
  let runner = now_h;
  cron.schedule(cron_str, runner, utils.cron_conf);
};

const run_cron_scheduled = async () => {
  console.log("run_cron_h");
  let cron_str = "*/2 * * * *";
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("Next run:", c_itvl.next().toISOString(), "\n");
  let runner = now_scheduled;
  cron.schedule(cron_str, runner, utils.cron_conf);
};

const test = async () => {
  // await zed_db.db.collection(coll2).createIndex({ hid: 1 }, { unique: true });
  await zed_db.db
    .collection("tourneyr01_sraces")
    .deleteMany({ thisclass: { $ne: 99 } });
};
const main = () => {};
const tourneyr01 = {
  test,
  run_cron,
  now,
  run_dur,
  now_h,
  run_cron_h,
  now_scheduled,
  run_cron_scheduled,
};
module.exports = tourneyr01;
