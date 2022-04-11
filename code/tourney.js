const cron = require("node-cron");
const _ = require("lodash");
const moment = require("moment");
const { zed_db, zed_ch } = require("./connection/mongo_connect");
const { get_entryfee_usd } = require("./utils/base");
const bulk = require("./utils/bulk");
const { getv, get_fee_tag, iso, nano, cron_conf } = require("./utils/utils");
const { print_cron_details } = require("./utils/cyclic_dependency");

const tcoll = "tourney_master";
const tcoll_horses = (tid) => `tourney::${tid}::horses`;
const tcoll_stables = (tid) => `tourney::${tid}::stables`;

const tfet_many = (coll, query, projection) => {
  if (projection) projection = { _id: 0, ...(projection || {}) };
  let arg2 = (projection && { projection }) || undefined;
  return zed_db.db.collection(coll).find(query, arg2).toArray();
};
const tfet_one_ = (coll, query, projection) => {
  if (projection) projection = { _id: 0, ...(projection || {}) };
  let arg2 = (projection && { projection }) || undefined;
  return zed_db.db.collection(coll).findOne(query, arg2);
};

const tm1 = (...a) => tfet_one_(tcoll, ...a.slice(1));
const tm2 = (...a) => tfet_many(tcoll, ...a.slice(1));
const th1 = (...a) => tfet_one_(tcoll_horses(a[0]), ...a.slice(1));
const th2 = (...a) => tfet_many(tcoll_horses(a[0]), ...a.slice(1));
const ts1 = (...a) => tfet_one_(tcoll_stables(a[0]), ...a.slice(1));
const ts2 = (...a) => tfet_many(tcoll_stables(a[0]), ...a.slice(1));
const tx2 = (...a) => tfet_many("payments", ...a.slice(1));

const get_tids = async (body) => {
  let active = getv(body, "active") || false;
  let query = {};
  if (active)
    query = {
      tourney_st: { $gte: moment().toISOString() },
      tourney_ed: { $lte: moment().add(-5, "minutes").toISOString() },
    };
  let docs = await tm2(null, {}, { tid: 1 });
  let tids = _.map(docs, "tid") || [];
  return tids;
};

const get_acive_tids = async () => {
  let docs = await tm2(null, {}, { tid: 1 });
  let tids = _.map(docs, "tid") || [];
  return tids;
};

const get_tdoc = async (tid) => {
  return tm1(null, { tid });
};

const calc_t_score = (rrow, tdoc) => {
  let tot = 0;
  for (let e of tdoc.score_cr) {
    // e of score_cr[] {
    //   thisclass: [],
    //   distance: [],
    //   fee_tag: [],
    //   flame: [ 0 ],
    //   pos: [ 3 ],
    //   score: 2
    // }
    if (_.isEmpty(e.thisclass));
    else if (!e.thisclass.includes(rrow.rc)) continue;
    if (_.isEmpty(e.distance));
    else if (!e.distance.includes(rrow.distance)) continue;
    if (_.isEmpty(e.flame));
    else if (!e.flame.includes(rrow.flame)) continue;
    if (_.isEmpty(e.pos));
    else if (!e.pos.includes(rrow.place)) continue;
    tot += e.score || 0;
  }
  return tot;
};

const run_t_horse = async (hid, tdoc, entry_date) => {
  let { tourney_st, tourney_ed } = tdoc;
  let st_mx = tourney_st > entry_date ? tourney_st : entry_date;
  const rcr = tdoc.race_cr;
  // console.log("tdoc.race_cr", rcr);
  let rquery = {
    6: hid,
    2: { $gte: st_mx, $lte: tourney_ed },
  };
  if (_.isEmpty(rcr.thisclass));
  else rquery[5] = { $in: [rcr.thisclass] };
  if (_.isEmpty(rcr.distance));
  else rquery[1] = { $in: [rcr.distance] };
  // if (_.isEmpty(rcr.fee_tag));
  // else rquery[20] = { $in: [rcr.thisclass] };

  // console.log(rquery);

  let races = await zed_ch.db
    .collection("zed")
    .find(rquery, {
      projection: {
        _id: 0,
        1: 1, // distance
        2: 1, // date
        3: 1, // entryfee
        4: 1, // raceid
        5: 1, // thisclass
        // 6: 1, // hid
        // 7: 1, // finishtime
        8: 1, // place
        // 9: 1, // name
        10: 1, // gate
        // 11: 1, // odds
        // 12: 1, // unknown
        13: 1, // flame
        // 14: 1, // fee_cat
        // 15: 1, // adjfinishtime
        // 16: 1, // tc
        // 20: 1, // fee_tag
        // 21: 1, // entryfee_usd
        // 23: 1, // pool
      },
    })
    .toArray();

  races = _.isEmpty(races)
    ? []
    : races.map((r) => {
        let date = r[2];
        let entryfee = parseFloat(r[3]);
        let entryfee_usd = get_entryfee_usd({ fee: entryfee, date });
        let fee_tag = get_fee_tag(entryfee_usd);
        let rrow = {
          rid: r[4],
          date,
          rc: r[5],
          distance: r[1],
          fee_tag,
          entryfee,
          entryfee_usd,
          gate: r[10],
          place: r[8],
          flame: r[13],
        };
        return rrow;
      });

  if (!_.isEmpty(rcr.fee_tag))
    races = races.filter((r) => rcr.fee_tag.includes(r.fee_tag));

  races = races.map((rrow) => {
    let score = calc_t_score(rrow, tdoc);
    rrow.score = score;
    return rrow;
  });

  let traces_n = races.length;
  let tot_score = _.sumBy(races, "score");
  if ([NaN, undefined, null].includes(tot_score)) tot_score = 0;
  let avg_score = (tot_score || 0) / (traces_n || 1);
  let update_doc = {
    hid,
    traces_n,
    tot_score,
    avg_score,
    races,
    entry_date,
  };
  return update_doc;
};

const run_t_give_ranks = (hdocs, tdoc) => {
  let mode = tdoc.score_mode;
  let k =
    (mode == "total" && "tot_score") || (mode == "avg" && "avg_score") || null;
  if (!k) return hdocs;
  hdocs = _.sortBy(hdocs, (i) => {
    let val = Number(i[k]);
    return [NaN, undefined, 0, null].includes(val) ? 1e14 : -val;
  });
  let i = 0;
  hdocs = _.map(hdocs, (e) => {
    let rank = null;
    if (e[k] != 0) rank = ++i;
    return { ...e, rank };
  });
  return hdocs;
};

const run_tid = async (tid) => {
  let tdoc = await get_tdoc(tid, {});
  // console.log(tdoc);
  let { tourney_st, tourney_ed, entry_st, entry_ed } = tdoc;

  let stables = await ts2(
    tid,
    {},
    { stable_name: 1, horses: 1, transactions: 1 }
  );
  let hids_all =
    _.chain(stables).map("horses").flatten().compact().value() || [];
  console.log("hids_all.len", hids_all.length);
  // console.log(hids_all);

  let pay_ids =
    _.chain(stables).map("transactions").flatten().compact().value() || [];

  let hids_exists = (await th2(tid, {}, { hid: 1 })) || [];
  hids_exists = _.map(hids_exists, "hid") || [];
  console.log("hids_exists.len", hids_exists.length);

  let hids_miss = _.difference(hids_all, hids_exists);
  console.log("hids_miss.len", hids_miss.length);

  let txns = await tx2(
    null,
    {
      // status_code: 1,
      service: tcoll_stables(tid),
      date: { $gte: entry_st, $lte: entry_ed },
      "meta_req.hids": { $elemMatch: { $in: hids_all } },
    },
    { status_code: 1, "meta_req.hids": 1, date: 1 }
  );

  let txns_paid = _.filter(txns, (i) => i.status_code == 1);
  let hids_paid = _.chain(txns_paid)
    .map("meta_req.hids")
    .flatten()
    .uniq()
    .value();
  let hids_not_paid = _.difference(hids_all, hids_paid);

  console.log("hids_paid.len", hids_paid.length);
  console.log("hids_not_paid.len", hids_not_paid.length);

  let hdate = {};
  _.forEach(txns, (e) => {
    let date = e.date;
    let hids = getv(e, "meta_req.hids");
    if (!_.isEmpty(hids)) for (let h of hids) hdate[h] = date;
  });

  let stable_hids_ob = _.chain(stables)
    .keyBy("stable_name")
    .mapValues("horses")
    .entries()
    .map(([stable_name, hids]) => {
      if (_.isEmpty(hids)) return null;
      return (hids || []).map((hid) => [hid, stable_name]);
    })
    .compact()
    .flatten()
    .fromPairs()
    .value();
  0;
  let hids_new = _.intersection(hids_paid, hids_miss);
  console.log("hids_new.len", hids_new.length);
  let horses_new_docs = _.chain(hids_new)
    .map((hid) => {
      hid = parseInt(hid);
      return {
        hid,
        enter_date: hdate[hid],
        stable_name: stable_hids_ob[hid],
        races: [],
        tot_score: 0,
        avg_score: 0,
        traces_n: 0,
      };
    })
    .value();

  await zed_db.db
    .collection(tcoll_horses(tid))
    .deleteMany({ hid: { $in: hids_not_paid } });

  await bulk.push_bulk(
    tcoll_horses(tid),
    horses_new_docs,
    `${tcoll_horses(tid)} new horses`
  );

  let i = 0;
  let update_ar = [];
  for (let chu of _.chunk(hids_paid, 25)) {
    let [a, b] = [chu[0], chu[chu.length - 1]];
    let chu_ar = await Promise.all(
      chu.map((hid) => run_t_horse(hid, tdoc, hdate[hid]))
    );
    update_ar.push(chu_ar);
    i += chu.length;
    console.log(`done ${i}:: ${a} -> ${b}`);
  }
  update_ar = _.flatten(update_ar);
  update_ar = run_t_give_ranks(update_ar, tdoc);
  console.table(update_ar);
  await bulk.push_bulk(
    tcoll_horses(tid),
    update_ar,
    `${tcoll_horses(tid)} new horses`
  );
};

const runner = async () => {
  const tids = await get_tids({ active: 1 });
  console.log("tids.len", tids.length);
  console.log("=>>", tids);
  for (let tid of tids) {
    console.log("\n\n==========");
    let st = iso();
    console.log(`tid:: ${tid} running`, st);
    await run_tid(tid);
    let ed = iso();
    console.log(`tid:: ${tid} ended`, ed);
    let took = (nano(ed) - nano(st)) / 1000;
    console.log(`tid:: ${tid} took:${took} seconds`);
  }
};

const cron_str = "* * * * *";
const run_cron = async () => {
  console.log("##run cron", "tourney");
  print_cron_details(cron_str);
  cron.schedule(cron_str, runner, cron_conf);
};

const test = async () => {
  console.log("test");
};

const main_runner = async (args) => {
  try {
    let [_node, _cfile, args1, arg2, arg3, arg4, arg5, arg6] = args;
    console.log("# --tourney ", iso());
    if (arg2 == "test") await test();
    if (arg2 == "runner") await runner();
    if (arg2 == "run_cron") await run_cron();
  } catch (err) {
    console.log(err);
  }
};

const tourney = {
  test,
  runner,
  run_cron,
  main_runner,
};

module.exports = tourney;
