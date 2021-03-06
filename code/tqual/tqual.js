const _ = require("lodash");
const { zed_db, zed_ch } = require("../connection/mongo_connect");
const { nano, iso, getv, cron_conf, dec } = require("../utils/utils");
const moment = require("moment");
const red = require("../connection/redis");
const {
  struct_race_row_data,
  jstr,
  jparse,
  print_cron_details,
} = require("../utils/cyclic_dependency");
const zedf = require("../utils/zedf");
const bulk = require("../utils/bulk");
const cron = require("node-cron");

const tcoll = "tquall_master";
let test_mode = false;

const get_tinfo = async (tid, projection = null) => {
  let q = { tid };
  let parr = _.isNil(projection) ? [q] : [q, { projection }];
  let doc = await zed_db.db.collection(tcoll).findOne(...parr);
  return doc;
};

const traces_getter = async (rar) => {
  let fins = await Promise.all(
    rar.map(async (e) => {
      let redid = `tqual_rar:${e.raceid}:${e.hid}`;
      let cache = await red.rget(redid);
      let cache_exists = _.isEmpty(cache) ? false : true;
      return [e.raceid, e.hid, redid, cache_exists, cache];
    })
  );
  if (test_mode) console.table(fins);

  let cache_races = _.chain(fins)
    .filter((e) => e[3] == true)
    .map(4)
    .compact()
    .value();
  if (test_mode) console.log("cache", cache_races.length);

  let rids_not = _.map(
    _.filter(fins, (e) => e[3] == false),
    0
  );
  if (test_mode) console.log("redids_not", rids_not.length);
  rids_not = _.uniq(rids_not);
  if (test_mode) console.log("rids_not", rids_not.length);

  let races = [];

  races = [...races, ...cache_races];

  let i = 0;
  for (let chu of _.chunk(rids_not, 20)) {
    let earaces = await zed_ch.db
      .collection("zed")
      .find({ 4: { $in: chu } })
      .toArray();
    earaces = struct_race_row_data(earaces);
    for (let r of earaces) {
      let redid = `tqual_rar:${r.raceid}:${r.hid}`;
      await red.rset(redid, r, 60 * 60);
    }
    races.push(earaces);
    i += chu.length;
    if (test_mode) console.log("got", i, jstr(chu));
  }
  races = _.flatten(races);
  if (test_mode) console.log("ALL", races.length);
  if (test_mode) console.table(races.slice(0, 10));

  return races;
};

const calc_t_score = (rrow, tdoc) => {
  // if (test_mode) console.log(rrow);
  let tot = 0;
  for (let [sidx, e] of _.entries(tdoc.score_cr)) {
    // e of score_cr[] {
    //   thisclass: [],
    //   distance: [],
    //   fee_tag: [],
    //   flame: [ 0 ],
    //   pos: [ 3 ],
    //   score: 2
    // }
    if (_.isEmpty(e.thisclass));
    else if (!e.thisclass.includes(rrow.thisclass)) {
      if (test_mode)
        console.log(rrow.rid, "exit rc", rrow.thisclass, "n", e.thisclass);
      continue;
    }
    if (_.isEmpty(e.distance));
    else if (!e.distance.includes(rrow.distance)) {
      if (test_mode)
        console.log(rrow.rid, "exit distance", rrow.distance, "n", e.distance);
      continue;
    }
    if (_.isEmpty(e.fee_tag));
    else if (!e.fee_tag.includes(rrow.fee_tag)) {
      if (test_mode)
        console.log(rrow.rid, "exit fee_tag", rrow.fee_tag, "n", e.fee_tag);
      continue;
    }
    if (_.isEmpty(e.flame));
    else if (!e.flame.includes(rrow.flame)) {
      if (test_mode)
        console.log(rrow.rid, "exit flame", rrow.flame, "n", e.flame);
      continue;
    }
    if (_.isEmpty(e.pos));
    else if (!e.pos.includes(rrow.place)) {
      if (test_mode)
        console.log(rrow.rid, "exit place", rrow.place, "n", e.pos);
      continue;
    }
    if (test_mode) console.log(rrow.rid, `(${sidx}) conforms`, e.score);
    tot += e.score || 0;
  }
  return tot;
};

const normal_races_do = async (hid, tdoc, races) => {
  if (test_mode) console.log(hid);
  if (test_mode) console.log(tdoc.score_cr);
  races = races.map((rrow) => {
    if (test_mode) console.table([rrow]);
    let score = calc_t_score(rrow, tdoc);
    if (test_mode) console.log(rrow.raceid, { score });
    rrow.score = score;
    return rrow;
  });

  races = _.sortBy(races, (r) => -nano(r.date));

  let racesn_ob = {};
  racesn_ob["all"] = races.length ?? 0;

  racesn_ob["10-14"] =
    _.filter(races, (r) => [1000, 1200, 1400].includes(parseInt(r.distance)))
      .length ?? 0;
  racesn_ob["16-20"] =
    _.filter(races, (r) => [1600, 1800, 2000].includes(parseInt(r.distance)))
      .length ?? 0;
  racesn_ob["22-26"] =
    _.filter(races, (r) => [2200, 2400, 2600].includes(parseInt(r.distance)))
      .length ?? 0;

  let tot_score = _.sumBy(races, "score");
  if ([NaN, undefined, null].includes(tot_score)) tot_score = 0;
  let avg_score = (tot_score || 0) / (racesn_ob["all"] || 1);

  let races_map = _.clone(races).map((e) => {
    return { raceid: e.raceid, score: e.score };
  });
  // races_map = races_map?.slice(0, 5);

  update_doc = {
    hid,
    racesn_ob,
    tot_score,
    avg_score,
    // races_map,
  };
  return update_doc;
};

const get_hdocs = async ({ tdoc, races }) => {
  if (_.isEmpty(races)) return [];
  let group = _.groupBy(races, "hid");
  let hdocs = {};
  for (let [hid, rs] of _.entries(group)) {
    hid = parseInt(hid);
    if (test_mode) console.log("HORSE", hid);
    hdocs[hid] = await normal_races_do(hid, tdoc, rs);
    if (test_mode) console.log(hdocs[hid]);
  }
  return hdocs;
};

const get_stable_ob = async (hid) => {
  const fn = async () => {
    let doc = (await zedf.horse(hid)) || {};
    let { owner = null, owner_stable_slug = null, owner_stable = null } = doc;
    return { hid, owner, owner_stable, owner_stable_slug };
  };
  const redid = `tqual:horse_stable:${hid}`;
  let ob = await red.rfn(redid, fn, 4 * 60 * 60);
  return ob;
};

const get_stable_map = async (hids) => {
  let ob = [];
  let i = 0;
  for (let chu of _.chunk(hids, 10)) {
    let ea = await Promise.all(chu.map((hid) => get_stable_ob(hid)));
    if (test_mode) console.table(ea);
    ob.push(ea);
    i += chu.length;
    if (test_mode) console.log("got ", i, "horse stables");
  }
  ob = _.flatten(ob);
  ob = _.keyBy(ob, "hid");
  if (test_mode) console.table(ob);
  return ob;
};

const get_tstatus = async (tdoc) => {
  if (_.isEmpty(tdoc)) return null;
  let { tourney_st, tourney_ed } = tdoc;
  let now = iso();
  if (_.inRange(nano(now), nano(tourney_st), nano(tourney_ed))) return "live";
  if (tourney_st > now) return "upcoming";
  if (tourney_ed < now) return "ended";
};

const run_tid = async (tid) => {
  console.log("running tid", tid);
  const tcoll_ref = await zed_db.db.collection(tcoll);
  let tdoc = await get_tinfo(tid, { leaderboard: 0 });
  if (!tdoc) return console.log("doc not found");
  if (test_mode) console.log(tdoc);
  const status = await get_tstatus(tdoc);
  console.log("status", status);
  await tcoll_ref.updateOne({ tid }, { $set: { status } });

  let [st, ed] = [tdoc.tourney_st, tdoc.tourney_ed];
  console.log("Getting", st, "->>", ed);

  let rquery = {};
  let tclass_type = getv(tdoc, `race_cr.tclasstype`) || [];

  if (tclass_type.includes("c0")) rquery = { 5: { $in: [0] } };
  if (tclass_type.includes("free-all"))
    rquery = { 3: { $eq: "0.0" }, 5: { $ne: 99 } };
  if (tclass_type.includes("paid-all"))
    rquery = { 3: { $ne: "0.0" }, 5: { $ne: 99 } };
  if (tclass_type.includes("open")) rquery = { 5: { $in: [1000] } };

  rquery;

  let rar = [];
  const use_cached_rids = false;
  for (let now = nano(st); now < Math.min(nano(ed), Date.now()); ) {
    let nst = iso(now);
    let ned = moment(nst).add(10, "minutes").toISOString();
    if (ned > ed) ned = ed;
    let redid = `tqual_rids:${tid}:${nst}->${ned}::${jstr(tclass_type)}`;
    let ea;
    ea = await red.rget(redid);
    if (use_cached_rids) {
      if (true || test_mode) console.log([nst, ned], "cache:: ", ea.length);
    } else {
      ea = await zed_ch.db
        .collection("zed")
        .find(
          {
            2: { $gte: nst, $lte: ned },
            ...rquery,
          },
          { projection: { 4: 1, 6: 1, _id: 0 } }
        )
        .toArray();

      ea = ea?.map((e) => {
        return {
          raceid: e[4],
          hid: e[6],
        };
      });
      if (true || test_mode) console.log([nst, ned], "got:: ", ea.length);
      await red.rset(redid, ea, 60 * 60);
    }
    rar.push(ea);
    now = nano(ned) + 1;
  }
  rar = _.flatten(rar);

  console.log("TOTAL race rows:", rar.length);
  let races = await traces_getter(rar);
  console.log("downloaded races required");

  let leaderboard = {
    "10-14": [],
    "16-20": [],
    "22-26": [],
  };

  let hdocs = await get_hdocs({ tdoc, races });
  let lim = 5;

  let mode = tdoc.score_mode;
  let type = tdoc.type;
  let scrk =
    (mode == "total" && "tot_score") || (mode == "avg" && "avg_score") || null;
  console.log({ type, mode });

  for (let [k, range] of [
    ["10-14", [1000, 1200, 1400]],
    ["16-20", [1600, 1800, 2000]],
    ["22-26", [2200, 2400, 2600]],
  ]) {
    let ar = _.chain(hdocs)
      .clone()
      .map((e) => {
        let { hid, racesn_ob, tot_score, avg_score } = e;
        avg_score = parseFloat(dec(avg_score, 2));
        tot_score = parseFloat(dec(tot_score, 2));
        let traces_n = e.racesn_ob[k];
        return { hid, tot_score, avg_score, traces_n };
      })
      .filter((e) => e.traces_n != 0)
      .sortBy((e) => {
        if (e.traces_n < lim) return 1e5 - e[scrk];
        return -parseFloat(e[scrk]) * 100 + -e.traces_n;
      })
      .value();

    let prev_rank = 0;
    let countmap = _.countBy(ar, (e) => `${e[scrk]}-${e.traces_n}`);
    // console.table(countmap);

    ar = _.map(ar, (e, idx) => {
      let rank;
      if (e.traces_n < lim) rank = null;
      else {
        let identicals = countmap[`${e[scrk]}-${e.traces_n}`];
        rank = prev_rank + identicals;
      }
      if (getv(ar, `${idx + 1}.${scrk}`) != e[scrk]) prev_rank = rank;
      return { ...e, rank };
    });

    leaderboard[k] = ar;
    if (test_mode) console.table(leaderboard[k]);
    if (test_mode) console.log(leaderboard[k][0]);
  }
  let hids = [
    ..._.map(leaderboard["10-14"], "hid"),
    ..._.map(leaderboard["16-20"], "hid"),
    ..._.map(leaderboard["22-26"], "hid"),
  ];

  console.log("hids.len", hids.length);
  const stable_map = await get_stable_map(hids);

  await tcoll_ref.updateOne({ tid }, { $set: { leaderboard, stable_map } });
  console.log("completed", tid);
  console.log("=======================\n\n");

  // console.log(
  //   _.compact(
  //     hdocs.map((e) => {
  //       if (e.racesn_ob["10-14"] < 5) return null;
  //       if (e.racesn_ob["16-20"] < 5) return null;
  //       if (e.racesn_ob["22-26"] < 5) return null;
  //       return { hd: e.hid, ...e.racesn_ob };
  //     })
  //   )
  // );
};

const status_updater = async () => {
  let now = iso();
  let docs = await zed_db.db
    .collection(tcoll)
    .find(
      {
        $or: [
          { status: { $exists: false } },
          { status: { $eq: null } },
          {
            $and: [
              {
                tourney_st: {
                  $gte: moment(now).add(-2, "minutes").toISOString(),
                },
              },
              {
                tourney_ed: {
                  $lte: moment(now).add(15, "minutes").toISOString(),
                },
              },
            ],
          },
        ],
      },
      { projection: { tid: 1, tourney_st: 1, tourney_ed: 1 } }
    )
    .toArray();
  let upd = [];
  for (let doc of docs) {
    let status = await get_tstatus(doc);
    let ea = { tid: doc.tid, status };
    if (test_mode) console.log(ea);
    upd.push(ea);
  }
  console.table(upd);
  await bulk.push_bulkc(tcoll, upd, "status_update", "tid");
};

const runner = async () => {
  const now = iso();
  let st = moment(now).add(-1, "minutes").toISOString();
  let ed = moment(now).add(15, "minutes").toISOString();
  // console.log([st, ed]);
  let active_tids = await zed_db.db
    .collection(tcoll)
    .find(
      {
        $or: [
          { status: "live" },
          {
            tourney_st: { $lte: st },
            tourney_ed: { $gte: ed },
          },
        ],
      },
      { projection: { _id: 0, tid: 1 } }
    )
    .toArray();
  if (_.isEmpty(active_tids)) return console.log("no actives");
  active_tids = _.map(active_tids, "tid");
  console.log("active_tids", active_tids);

  for (let tid of active_tids) {
    try {
      await run_tid(tid);
    } catch (err) {
      console.log("err at tid", tid, "\n", err);
    }
  }
};

const run_cron = async () => {
  let running = 0;
  const fn = async () => {
    if (running) console.log("skip... aleady running");
    running = 1;
    try {
      await status_updater();
      await runner();
    } catch (err) {
      console.log(err);
    } finally {
      running = 0;
    }
  };

  const cron_str = "*/2 * * * *";
  console.log("##run cron", "tqual");
  print_cron_details(cron_str);
  cron.schedule(cron_str, fn, cron_conf);
};

const test = async () => {
  console.log("test");
  let hdocs = await red.rget("tqual:test:hdocs:01");
  console.log("hdoc.len", _.keys(hdocs).length);

  let tdoc = await get_tinfo("8cf1aff0");
  let lim = 5;

  let mode = tdoc.score_mode;
  let type = tdoc.type;
  let scrk =
    (mode == "total" && "tot_score") || (mode == "avg" && "avg_score") || null;
  console.log({ type, mode });

  let leaderboard = {
    "10-14": [],
    "16-20": [],
    "22-26": [],
  };
  for (let [k, range] of [
    ["10-14", [1000, 1200, 1400]],
    ["16-20", [1600, 1800, 2000]],
    ["22-26", [2200, 2400, 2600]],
  ]) {
    let ar = _.chain(hdocs)
      .clone()
      .map((e) => {
        let { hid, racesn_ob, tot_score, avg_score } = e;
        avg_score = parseFloat(dec(avg_score, 2));
        tot_score = parseFloat(dec(tot_score, 2));
        let traces_n = e.racesn_ob[k];
        return { hid, tot_score, avg_score, traces_n };
      })
      .filter((e) => e.traces_n != 0)
      .sortBy((e) => {
        if (e.traces_n < lim) return 1e5 - e[scrk];
        return -parseFloat(e[scrk]) * 100 + -e.traces_n;
      })
      .value();

    let prev_rank = 0;
    let countmap = _.countBy(ar, (e) => `${e[scrk]}-${e.traces_n}`);
    // console.table(countmap);

    ar = _.map(ar, (e, idx) => {
      let rank;
      if (e.traces_n < lim) rank = null;
      else {
        let identicals = countmap[`${e[scrk]}-${e.traces_n}`];
        rank = prev_rank + identicals;
      }
      if (getv(ar, `${idx + 1}.${scrk}`) != e[scrk]) prev_rank = rank;
      return { ...e, rank };
    });

    leaderboard[k] = ar;
    console.table(leaderboard[k]);
    console.log(leaderboard[k][0]);
  }

  return console.log("end test");
};

const main_runner = async () => {
  console.log("tqual");
  let [_node, _cfile, arg1, arg2, arg3, arg4, arg5] = process.argv;
  if (process.argv.includes("test_mode")) {
    test_mode = true;
    console.log({ test_mode });
  }
  if (arg2 == "test") await test();
  if (arg2 == "status") await status_updater();
  if (arg2 == "run") await run_tid(arg3);
  if (arg2 == "runner") await runner();
  if (arg2 == "cron") await run_cron();
};

const tqual = {
  main_runner,
};
module.exports = { tqual };
