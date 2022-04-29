const cron = require("node-cron");
const _ = require("lodash");
const moment = require("moment");
const { zed_db, zed_ch } = require("../connection/mongo_connect");
const { get_entryfee_usd } = require("../utils/base");
const bulk = require("../utils/bulk");
const { getv, get_fee_tag, iso, nano, cron_conf } = require("../utils/utils");
const { print_cron_details, jparse } = require("../utils/cyclic_dependency");
const utils = require("../utils/utils");
const {
  tcoll,
  tcollp,
  tcoll_horses,
  tcoll_stables,
  payout_list,
  get_p,
  create_t,
  get_leaderboard_t,
  payout_single,
} = require("./depend");
const send_weth = require("../payments/send_weth");

let test_mode = 0;
let running = 0;

// const tcoll = "tourney_master";
// const tcoll_horses = (tid) => `tourney::${tid}::horses`;
// const tcoll_stables = (tid) => `tourney::${tid}::stables`;

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

const get_flash_run_tids = async () => {
  let query = {
    $or: [
      { type: "flash", status: { $in: ["open", "live"] } },
      {
        type: "flash",
        status: "ended",
        tourney_ed: { $gte: moment().add(-5, "minutes").toISOString() },
        payout_done: { $ne: true },
      },
    ],
  };
  let docs = await zed_db.db
    .collection(tcoll)
    .find(query, { _id: 0, tid: 1 })
    .toArray();
  return _.map(docs, "tid") || [];
};

const get_tids = async (body) => {
  let active = getv(body, "active") || false;
  let query = {};
  if (active) {
    query = {
      $or: [
        {
          type: "regular",
          tourney_st: { $lte: moment().toISOString() },
          tourney_ed: { $gte: moment().add(-30, "minutes").toISOString() },
        },
        {
          type: "regular",
          entry_st: { $lte: moment().toISOString() },
          entry_ed: { $gte: moment().add(-30, "minutes").toISOString() },
        },
        // {
        //   type: "flash",
        //   status: "open",
        //   entry_st: { $lte: moment().toISOString() },
        // },
        // {
        //   type: "flash",
        //   status: "live",
        //   tourney_ed: { $gte: moment().add(-30, "minutes").toISOString() },
        // },
      ],
    };
    // console.log(query);
  }
  let docs = await tm2(null, query, { tid: 1 });
  let tids = _.map(docs, "tid") || [];
  return tids;
};

const get_tdoc = async (tid) => {
  return tm1(null, { tid });
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
    else if (!e.thisclass.includes(rrow.rc)) {
      if (test_mode)
        console.log(rrow.rid, "exit rc", rrow.rc, "n", e.thisclass);
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

const get_horse_entry_date = async (hid, tdoc) => {
  let { entry_st, entry_ed, tid } = tdoc;
  let txns = await tx2(
    null,
    {
      status_code: 1,
      service: tcoll_stables(tid),
      date: {
        $gte: entry_st,
        ...(entry_ed ? { $lte: entry_ed } : {}),
      },
      "meta_req.hids": { $elemMatch: { $in: [hid] } },
      "meta_req.type": "fee",
    },
    { pay_id: 1, status_code: 1, req_amt: 1, "meta_req.hids": 1, date: 1 }
  );
  if (getv(txns, "0.status_code") == 1) return getv(txns, "0.date");
  return false;
};

const run_t_horse = async (hid, tdoc, entry_date) => {
  if (!entry_date) {
    console.log("cant find entrydate of horse", hid, tdoc.tid);
    return;
  }
  let { tourney_st, tourney_ed } = tdoc;
  let st_mx = tourney_st > entry_date ? tourney_st : entry_date;
  const rcr = tdoc.race_cr;
  if (test_mode) console.log("tdoc.race_cr", rcr);
  let rquery = {
    6: hid,
    2: { $gte: st_mx, $lte: tourney_ed },
  };
  if (_.isEmpty(rcr.thisclass));
  else rquery[5] = { $in: rcr.thisclass };
  if (_.isEmpty(rcr.distance));
  else rquery[1] = { $in: rcr.distance };
  // if (_.isEmpty(rcr.fee_tag));
  // else rquery[20] = { $in: [rcr.thisclass] };

  if (test_mode) console.log(JSON.stringify(rquery, "", 4));

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
          place: parseFloat(r[8]),
          flame: r[13],
        };
        return rrow;
      });

  races = _.uniqBy(races, (i) => i.rid);
  if (!_.isEmpty(races)) races = _.sortBy(races, (r) => -nano(r.date));
  if (test_mode) console.log(_.map(races, "rid"));

  if (!_.isEmpty(rcr.fee_tag))
    races = races.filter((r) => rcr.fee_tag.includes(r.fee_tag));

  if (tdoc.type == "flash") races = races.slice(0, 5);

  races = races.map((rrow) => {
    let score = calc_t_score(rrow, tdoc);
    rrow.score = score;
    return rrow;
  });
  if (test_mode) console.table(races);

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
  if (test_mode) console.log(update_doc);
  return update_doc;
};

const run_t_give_ranks = (hdocs, tdoc) => {
  let mode = tdoc.score_mode;
  let k =
    (mode == "total" && "tot_score") || (mode == "avg" && "avg_score") || null;
  if (!k) return hdocs;
  hdocs = _.sortBy(hdocs, (i) => {
    let val = Number(i[k]);
    let n = Number(i["traces_n"]);
    if (!n) return 1e14;
    return [NaN, undefined, 0, null].includes(val) ? -n : -(val * 1000 + n);
  });
  let i = 0;
  hdocs = _.map(hdocs, (e) => {
    let rank = null;
    if (e[k] != 0 && e.traces_n >= 5) rank = ++i;
    return { ...e, rank };
  });
  if (test_mode) console.log(hdocs);
  return hdocs;
};

const run_t_tot_fees = async (tid, tdoc) => {
  let { tourney_st, tourney_ed, entry_st, entry_ed } = tdoc;
  let pays = await zed_db.db
    .collection("payments")
    .find(
      {
        status_code: 1,
        service: tcoll_stables(tid),
        date: { $gte: entry_st, $lte: entry_ed },
        "meta_req.type": { $in: ["fee", "sponsor"] },
      },
      { projection: { req_amt: 1, "meta_req.type": 1 } }
    )
    .toArray();
  // console.log(pays);
  let tot_fees =
    _.chain(pays)
      .filter((i) => getv(i, "meta_req.type") == "fee")
      .map("req_amt")
      .filter((e) => ![null, undefined, NaN].includes(parseFloat(e)))
      .sum()
      .value() ?? 0;
  let tot_sponsors =
    _.chain(pays)
      .filter((i) => getv(i, "meta_req.type") == "sponsor")
      .map("req_amt")
      .filter((e) => ![null, undefined, NaN].includes(parseFloat(e)))
      .sum()
      .value() ?? 0;
  return { tot_fees, tot_sponsors };
};

// upcoming => entry not started
// open => entry started
// live => tourney started running
// ended => tourney ended
const t_status = async () => {
  let docs = await zed_db.db
    .collection(tcoll)
    .find(
      {
        $or: [
          { tourney_ed: { $gte: moment().add(-1, "days").toISOString() } },
          { tourney_ed: { $eq: null } },
        ],
      },
      {
        projection: {
          _id: 0,
          tid: 1,
          type: 1,
          tourney_st: 1,
          tourney_ed: 1,
          entry_st: 1,
          entry_ed: 1,
          status: 1,
        },
      }
    )
    .toArray();
  let now = iso();
  let ar = docs.map((e) => {
    let { tid, tourney_st, type, tourney_ed, entry_st, entry_ed, status } = e;
    let upd = { tid };
    if (type == "flash" && !tourney_ed && !tourney_st) {
      if (entry_st < now) status = "open";
      if (entry_st > now) status = "upcoming";
    } else {
      if (tourney_ed < now) status = "ended";
      if (_.inRange(nano(now), nano(tourney_st), nano(tourney_ed)))
        status = "live";
      if (_.inRange(nano(now), nano(entry_st), nano(entry_ed))) status = "open";
      if (now < entry_st) status = "upcoming";
    }
    upd.status = status;
    return upd;
  });
  console.table(ar);
  await bulk.push_bulkc(tcoll, ar, "tourney:t_status", "tid");
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
      date: {
        $gte: entry_st,
        ...(entry_ed ? { $lte: entry_ed } : {}),
      },
      "meta_req.hids": { $elemMatch: { $in: hids_all } },
      "meta_req.type": "fee",
    },
    { pay_id: 1, status_code: 1, req_amt: 1, "meta_req.hids": 1, date: 1 }
  );

  // const pay_ids2 = _.map(txns, "pay_id");
  // console.log(pay_ids);
  // await zed_db.db
  //   .collection("payments")
  //   .updateMany(
  //     { pay_id: { $in: pay_ids2 } },
  //     { $set: { "meta_req.type": "fee" } }
  //   );
  // return;

  let txns_paid = _.filter(txns, (i) => i.status_code == 1);
  let pays_doc = await run_t_tot_fees(tid, tdoc);
  let total_capital = pays_doc.tot_fees + pays_doc.tot_sponsors;
  let prize_pool = parseFloat(utils.dec(total_capital * 0.95, 4));
  let fins = {
    ...pays_doc,
    total_capital,
    prize_pool,
  };
  console.log(fins);

  let hids_paid = _.chain(txns_paid)
    .map("meta_req.hids")
    .flatten()
    .uniq()
    .value();
  let horses_n = hids_paid?.length || 0;
  console.log({ horses_n });
  fins.horses_n = horses_n;
  await zed_db.db.collection(tcoll).updateOne({ tid }, { $set: fins });

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

  let n = hids_paid.length;
  if (tdoc.type == "flash") {
    if (n >= tdoc.flash_params.minh) {
      let { tourney_st, tourney_ed, entry_st, entry_ed, flash_params } = tdoc;
      if (!tourney_st && !tourney_ed) {
        let upd = {
          tourney_st: iso(),
          tourney_ed: moment()
            .add(flash_params.duration, "hours")
            .toISOString(),
          entry_ed: iso(),
        };
        tdoc = { ...tdoc, ...upd };
        await zed_db.db.collection(tcoll).updateOne({ tid }, { $set: upd });
      }
    } else {
      let upd = {
        tourney_st: null,
        tourney_ed: null,
        entry_ed: null,
      };
      tdoc = { ...tdoc, ...upd };
      await zed_db.db.collection(tcoll).updateOne({ tid }, { $set: upd });
    }
  }

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
  if (test_mode) console.table(update_ar);
  await bulk.push_bulk(
    tcoll_horses(tid),
    update_ar,
    `${tcoll_horses(tid)} new horses`
  );
};

const cron_str = "* * * * *";
const run_cron = async () => {
  console.log("##run cron", "tourney");
  print_cron_details(cron_str);
  cron.schedule(cron_str, runner, cron_conf);
};

const thorse = async ([tid, hid]) => {
  test_mode = 1;
  // let tid = "f3effc3a";
  // let hid = 200985;
  hid = parseInt(hid);
  if (_.isNaN(hid)) {
    console.log("NAN hid");
    return;
  }
  let tdoc = await get_tdoc(tid);
  let entry_date = await get_horse_entry_date(hid, tdoc);
  let doc = await run_t_horse(hid, tdoc, entry_date);
  console.log(doc);
};

const run = async (tid) => {
  await t_status();
  await run_tid(tid);
};

const regular_runner = async () => {
  try {
    const tids = await get_tids({ active: true });
    console.log("tids.len", tids.length);
    console.log("=>>", tids);
    for (let tid of tids) {
      try {
        console.log("\n\n==========");
        let st = iso();
        console.log(`tid:: ${tid} running`, st);
        await run_tid(tid);
        let ed = iso();
        console.log(`tid:: ${tid} ended`, ed);
        let took = (nano(ed) - nano(st)) / 1000;
        console.log(`tid:: ${tid} took:${took} seconds`);
      } catch (err) {
        console.log(err);
      }
    }
  } catch (err) {
    console.log(err);
  }
};

const flash_run_current = async () => {
  const tids = await get_flash_run_tids();
  console.log("# flash_run_current", tids.length, tids);
  for (let tid of tids) {
    try {
      console.log("\n\n==========");
      let st = iso();
      console.log(`tid:: ${tid} running`, st);
      await run_tid(tid);
      let ed = iso();
      console.log(`tid:: ${tid} ended`, ed);
      let took = (nano(ed) - nano(st)) / 1000;
      console.log(`tid:: ${tid} took:${took} seconds`);
    } catch (err) {
      console.log(err);
    }
  }
};

const calc_payouts_list = async ({ tid }) => {
  let tdoc = await get_tdoc(tid);
  let { prize_pool, payout_mode, score_mode } = tdoc;
  // prize_pool = 1;
  console.log({ prize_pool, payout_mode, score_mode });
  let k =
    (score_mode == "total" && "tot_score") ||
    (score_mode == "avg" && "avg_score") ||
    null;
  let leader = await get_leaderboard_t({ tid });
  if (leader.length == 0) return [];

  let pays = [];

  if (payout_mode == "winner_all") {
    console.table(leader);
    if (!leader[0].rank) return [];
    return [{ ...leader[0], amt: prize_pool }];
  } else if (payout_mode == "double_up") {
    let mid = parseInt(leader.length / 2);
    leader = _.filter(
      leader,
      (i) => ![0, "na", null, undefined, NaN].includes(i.rank)
    );
    if (mid > leader.length) {
      console.log("less than mid");
      pays = [...leader.map((e) => ({ ...e, amt }))];
      return pays;
    }
    let last = leader[mid - 1];
    let last_ties = _.filter(leader, (i) => i[k] == last[k]);
    leader = leader.slice(0, mid);

    if (last_ties.length > 1) {
      console.log("last_ties_hids mid:", mid);
      let last_ties_hids = _.map(last_ties, "hid");
      let top_half = _.filter(
        leader.slice(0, mid),
        (i) => !last_ties_hids.includes(i.hid)
      );
      console.table(last_ties);
      console.table(top_half);
      let totn = mid;

      let top_half_n = top_half.length;
      let amt = prize_pool / (top_half_n + 1);

      let ties_n = last_ties.length;
      let tie_sec = amt;
      let tie_amt = tie_sec / ties_n;

      console.log({
        amt,
        mid,
        top_half_n,
        ties_n,
        tie_amt,
      });
      pays = [
        ...top_half.map((e) => ({ ...e, amt })),
        ...last_ties.map((e) => ({ ...e, amt: tie_amt })),
      ];
    } else {
      console.log("no tie", mid);
      let amt = prize_pool / leader.length;
      pays = [...leader.map((e) => ({ ...e, amt }))];
    }
  }
  return pays;
};

const flash_payout = async (tid) => {
  console.log("flash_payout");
  console.log(`## [ ${tid} ] started payout`, iso());
  let pays = await calc_payouts_list({ tid });
  console.table(pays);
  if (_.isEmpty(pays)) {
    console.log("nothing to payout");
  } else {
    let adwallet = process.env.flash_payout_wallet;
    await Promise.all(
      pays.map((l) =>
        payout_single({
          tid,
          payout_wallet: adwallet,
          stable_name: l.stable_name,
          wallet: l.wallet,
          amt: l.amt,
        })
      )
    );
    let payments = pays.map((l) => ({
      WALLET: l.wallet,
      AMOUNT: l.amt.toString(),
    }));
    console.table(payments);
    let key = process.env.flash_payout_private_key;
    let count = await send_weth.sendAllTransactions(payments, key);
    // let count = 0;
    console.log("done transactions:", count);
  }
  await zed_db.db
    .collection(tcoll)
    .updateOne({ tid }, { $set: { payout_done: true, payout_date: iso() } });
  console.log(`## [ ${tid} ] ended payout`, iso());
};

const flash_auto_start = async () => {
  let pids_active = await zed_db.db
    .collection(tcollp)
    .find(
      { pid: { $regex: /^flash/i }, flash_preset_active: true },
      { projection: { pid: 1, _id: 0 } }
    )
    .toArray();
  pids_active = _.map(pids_active, "pid");
  console.log("# flash_auto_start");
  console.log("pids active", pids_active.length, pids_active);
  let tids_doc = await zed_db.db
    .collection(tcoll)
    .find(
      {
        type: "flash",
        status: { $in: ["open", "live"] },
      },
      { projection: { tid: 1, _id: 0, created_using: 1 } }
    )
    .toArray();
  let pids_running =
    _.chain(tids_doc).map("created_using").uniq().value() || [];
  console.log("pids running", pids_running.length, pids_running);

  let pids_consider = _.difference(pids_active, pids_running);
  console.log("pids considering", pids_consider.length, pids_consider);
  for (let pid of pids_consider) {
    try {
      console.log(`creating flash tourney using ${pid}`);
      let doc = await get_p({ pid });
      doc.created_using = pid;
      doc.entry_st = iso();
      let resp = await create_t(doc);
      console.log(resp?.msg);
    } catch (err) {}
  }
};

const flash_payout_ended = async () => {
  let tids = await zed_db.db
    .collection(tcoll)
    .find(
      {
        status: "ended",
        type: "flash",
        payout_done: { $ne: true },
        tourney_ed: {
          $lte: moment().add(-5, "minutes").toISOString(),
          $gte: moment().add(-15, "minutes").toISOString(),
        },
      },
      { projection: { tid: 1, _id: 0 } }
    )
    .toArray();
  tids = _.map(tids, "tid");
  console.log("# flash_payout_ended", tids.length, tids);
  try {
    for (let tid of tids) {
      try {
        await flash_payout(tid);
      } catch (err) {
        console.log("Flash_payout err", tid, err.message);
      }
    }
  } catch (err) {
    console.log(err);
  }
};

const flash_runner = async () => {
  await flash_run_current();
  await flash_payout_ended();
  await flash_auto_start();
};

const runner = async () => {
  if (running) {
    console.log("############# tourney already running.........");
    return;
  }
  try {
    running = 1;
    await t_status();
    await regular_runner();
    await flash_runner();
    await t_status();
    running = 0;
  } catch (err) {
    console.log("TOURNEY ERR\n", err);
    running = 0;
  }
};

const main_runner = async (args) => {
  try {
    let [_node, _cfile, args1, arg2, arg3, arg4, arg5, arg6] = args;
    console.log("# --tourney ", iso());
    if (arg2 == "test") await test();
    if (arg2 == "thorse") await thorse([arg3, arg4]);
    if (arg2 == "run") await run(arg3);
    if (arg2 == "runner") await runner();
    if (arg2 == "flash_runner") await flash_runner();
    if (arg2 == "run_cron") await run_cron();
    if (arg2 == "t_status") await t_status();
  } catch (err) {
    console.log(err);
  }
};

const test = async () => {
  // let tid = "aaa4c417";
  let tid = "683af5c9";
  await flash_payout(tid);
};

const tourney = {
  test,
  thorse,
  runner,
  flash_runner,
  run,
  run_cron,
  main_runner,
  t_status,
};

module.exports = tourney;
