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

const coll = "tourney02";
const coll2 = "tourney02_leader";
const coll3 = "tourney02_sraces";
const dur = 2.2 * 60 * 1000;
const test_mode = 0;

let t_st_date = "2022-02-04T16:00:00.000Z";
let t_ed_date = "2022-02-06T00:00:00.000Z";

let stable_ob = [];
let active_hids = [];
let all_hids = [];
let leader_old = [];

let get_points = {
  1: 6,
  2: 3,
  3: 2,
  4: 1,
};

const get_horse_poins = async (hid, lim = 8) => {
  let stable_eaob = get_stable_eaob(hid);
  let st_date = t_st_date;
  let hids_ob = stable_eaob.hids_ob || {};
  let h_date = hids_ob[hid] || stable_eaob.date;
  if (h_date > t_st_date) {
    st_date = h_date;
  }
  let races =
    (await zed_ch.db
      .collection("zed")
      .find(
        { 2: { $gte: st_date, $lte: t_ed_date }, 6: hid },
        { projection: { 6: 1, 8: 1 } }
      )
      .sort({ 2: 1 })
      .limit(lim)
      .toArray()) ?? [];
  // let filt_races = filter_races(races,
  let poss = _.map(races, (i) => parseFloat(i[8]));
  let pts = poss.reduce((acc, e) => acc + (get_points[e] ?? 0), 0);
  if (poss.length == 0) pts = 0;
  let traces_n = poss.length;
  console.log(`::${hid} #${traces_n}`, { pts });
  return { hid, traces_n, pts };
};

const get_stable_ob = async () => {
  let ob = await zed_db.db
    .collection(coll)
    .find(
      { stable_name: { $ne: null } }
      // { projection: { active: 1, stable_name: 1, hids: 1 } }
    )
    .toArray();
  return ob;
};
const update_hids_list = async () => {
  stable_ob = await get_stable_ob();
  all_hids = _.chain(stable_ob)
    .map("hids")
    .flatten()
    .uniq()
    .compact()
    .map((e) => parseInt(e))
    .value();
  active_hids = _.chain(stable_ob)
    .filter({ active: true })
    .map("hids")
    .flatten()
    .uniq()
    .compact()
    .map((e) => parseInt(e))
    .value();
  console.log("stables:", stable_ob.length);
  console.log("all_hids   :", all_hids.length);
  console.log("active_hids:", active_hids.length);

  //delete not actives
  await zed_db.db
    .collection(coll2)
    .deleteMany({ hid: { $not: { $in: active_hids } } });

  leader_old =
    (await zed_db.db
      .collection(coll2)
      .find({ hid: { $exists: true, $in: all_hids } })
      .sort({ pts: -1, traces_n: -1 })
      .toArray()) ?? [];
};

const generate_leader = async () => {
  let leader = [];
  let gp = _.groupBy(leader_old, (e) => {
    return `${e.traces_n}-${e.pts}`;
  });
  for (let chu of _.chunk(active_hids, 5)) {
    let ar = await Promise.all(
      chu.map((hid) => {
        let { traces_n = 0, pts = 0 } = _.find(leader_old, { hid }) ?? {};
        let lim = 8;
        let ns = gp[`${traces_n}-${pts}`]?.length ?? 0;
        if (traces_n >= 8) console.log(hid, ns);
        if (traces_n == 0) lim = 8;
        // if (traces_n >= 8 && ns > 1) {
        //   lim = Math.max(8, traces_n + 1);
        // }
        return get_horse_poins(hid, lim);
      })
    );
    leader.push(ar);
  }
  leader = _.flatten(leader);
  leader = leader.map((e) => {
    return { ...e, stable_name: get_stable_name(e.hid) };
  });
  let bulk = [];
  for (let ea of leader) {
    if (_.isEmpty(ea)) continue;
    bulk.push({
      updateOne: {
        filter: { hid: ea.hid },
        update: { $set: ea },
        upsert: true,
      },
    });
  }
  if (test_mode !== 1) await zed_db.db.collection(coll2).bulkWrite(bulk);
};

const get_stable_name = (hid) => {
  let ob = _.chain(stable_ob).keyBy("stable_name").mapValues("hids").value();
  for (let [stable, hids] of _.entries(ob)) {
    if (hids.includes(hid)) return stable;
  }
  return "na-stable";
};
const get_stable_eaob = (hid) => {
  let ob = _.chain(stable_ob).keyBy("stable_name").value();
  for (let [stable, stable_eaob] of _.entries(ob)) {
    // console.log(stable, stable_eaob);
    let hids = stable_eaob?.hids || [];
    if (hids.includes(hid)) return stable_eaob;
  }
  return {};
};

const now_h = async () => {
  console.log("init");
  await update_hids_list();
  console.log("now_h:", iso());
  // for (let chu of _.chunk(all_hids, 25)) {
  //   await Promise.all(chu.map(calc_horse_points));
  // }
  await generate_leader();
  console.log("end:", iso(), "\n----------");
};

const run_cron_h = async () => {
  console.log("run_cron_h");
  let cron_str = "*/1 * * * *";
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("Next run:", c_itvl.next().toISOString(), "\n");
  let runner = now_h;
  cron.schedule(cron_str, runner, utils.cron_conf);
};

const test = async () => {};
const main = () => {};
const tourneyr02 = {
  test,
  now_h,
  run_cron_h,
};
module.exports = tourneyr02;
