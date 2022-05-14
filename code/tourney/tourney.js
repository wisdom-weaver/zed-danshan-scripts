const cron = require("node-cron");
const _ = require("lodash");
const moment = require("moment");
const { zed_db, zed_ch } = require("../connection/mongo_connect");
const { get_entryfee_usd } = require("../utils/base");
const bulk = require("../utils/bulk");
const {
  getv,
  get_fee_tag,
  iso,
  nano,
  cron_conf,
  cdelay,
} = require("../utils/utils");
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
  get_double_up_list,
  get_winner_all_list,
  refund_user,
  flash_payout_wallet,
  flash_payout_private_key,
  get_elo_score,
} = require("./depend");
const send_weth = require("../payments/send_weth");
const { fget } = require("../utils/fetch");

let test_mode = 0;
let running = 0;
let frunning = 0;
let eth_price = 0;
let payout_test = 0;
let eval_hidden = 1;
// const tcoll = "tourney_master";
// const tcoll_horses = (tid) => `tourney::${tid}::horses`;
// const tcoll_stables = (tid) => `tourney::${tid}::stables`;

const max_elo_races = 10;

const update_eth = async () => {
  let ob = await fget(
    `https://min-api.cryptocompare.com/data/price?fsym=ETH&tsyms=BTC,USD`
  );
  eth_price = ob.USD;
};
const eth_t_usd = (c) => {
  return c * eth_price;
};

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

const ft_price_ob = {
  // FT0 ["$0", ],
  // FTA: ["ALL", "bg-purple-500", -1e14, 1e14],
  FT1: ["$2", "bg-green-500", -1, 2],
  FT2: ["$5", "bg-green-400", 2, 5],
  FT3: ["$10", "bg-yellow-400", 5, 10],
  FT4: ["$25", "bg-yellow-600", 10, 25],
  FT5: ["$50", "bg-orange-500", 25, 50],
  FT6: ["$100", "bg-red-400", 50, 100],
  FT7: ["$250", "bg-red-500", 100, 250],
  FT8: ["$500", "bg-red-600", 250, 500],
  FT9: ["ultimate", "bg-red-800 shadow-md shadow-purple-500", 500, 1e14],
};
const get_ft = (usd) => {
  if (usd == "multi") return "multi";
  let ob = _.entries(ft_price_ob).find(([k, [disp, cn, mi, mx]]) => {
    // console.log(k, mi, mx, _.inRange(usd, mi, mx));
    if (_.inRange(usd, mi, mx)) return true;
  });
  return (ob && ob[0]) || null;
};

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

const track_horse_elo = async ({ hid, tdoc, elo_list }) => {
  let elo_curr = await get_elo_score(hid);
  let ea = { elo_curr, elo_time: iso() };
  if (_.isEmpty(elo_list)) return [ea];
  let top = elo_list[elo_list.length - 1]?.elo_curr;
  if (elo_curr == top) return elo_list;
  else {
    let leader_hdoc_ref = zed_db.db.collection(tcoll_horses(tdoc.tid));
    elo_list = [...elo_list, ea];
    await leader_hdoc_ref.updateOne({ hid }, { $set: { elo_list } });
    return elo_list;
  }
};

const get_opt_elo_from_list = (date, elo_list) => {
  date = nano(date);
  if (_.isEmpty(elo_list)) return null;
  let ob = _.minBy(elo_list, (e) => {
    if (date > nano(e.elo_time)) return 1e18;
    return e.elo_time - date;
  });
  return ob.elo_curr ?? null;
};

const elo_races_do = async (hid, tdoc, races) => {
  const leader_hdoc_ref = zed_db.db.collection(tcoll_horses(tdoc.tid));
  let hdoc = await leader_hdoc_ref.findOne(
    { hid },
    { projection: { hid: 1, elo_init: 1, entry_date: 1, elo_list: 1 } }
  );
  // console.log(hdoc);
  let now = iso();
  let st = tdoc.tourney_st;
  let diff = moment(st).diff(moment(now), "minutes");
  let elo_init = hdoc?.elo_init || null;
  let elo_last = hdoc?.elo_last || null;
  let elo_list = hdoc?.elo_list || [];
  // console.log("#diff", diff);
  if (_.inRange(diff, 0, 5)) {
    console.log("##eval inside");
    elo_init = await get_elo_score(hid);
    await leader_hdoc_ref.updateOne(
      { hid },
      {
        $set: {
          elo_init,
          elo_list: [{ id: "init", elo_curr: elo_init, elo_time: iso(now) }],
        },
      }
    );
    elo_last = null;
  }
  if (st < now) {
    elo_list = await track_horse_elo({ hid, tdoc, elo_list });
    // console.table(elo_list);
  }
  if (_.isEmpty(races)) return { elo_last: null, traces_n: 0, elo_score: null };
  races = _.sortBy(races, "date");
  races = races.slice(0, max_elo_races);
  let traces_n = races.length;
  for (let i = 0; i < traces_n; i++) {
    let race = races[i];
    if (!race.hrating)
      race.hrating = get_opt_elo_from_list(race.date, elo_list);

    races[i].score =
      i == 0 ? race.hrating - elo_init : race.hrating - races[i - 1].hrating;
    if (i == traces_n - 1) elo_last = race.hrating;
    races[i].score = -races[i].score;
  }
  // console.table(races);
  // console.table(elo_list);
  let elo_score = -(elo_last - elo_init);
  // console.log({ elo_init, elo_last });
  return { hid, traces_n, elo_score, races, elo_last };
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
        6: 1, // hid
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
        // 19: 1, // fee_tag
        // 20: 1, // prize
        // 21: 1, // prize_usd
        22: 1, // hrating
      },
    })
    .sort({ 2: 1 })
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
          hid: r[6],
          date,
          rc: r[5],
          distance: r[1],
          fee_tag,
          entryfee,
          entryfee_usd,
          gate: r[10],
          place: parseFloat(r[8]),
          flame: r[13],
          hrating: r[22],
        };
        return rrow;
      });

  let update_doc = { hid, entry_date };

  const is_elo_mode = tdoc.score_mode == "elo";
  if (!is_elo_mode) {
    races = _.uniqBy(races, (i) => i.rid);
    if (test_mode) console.log("type:", tdoc.type);
    if (test_mode) console.log("a0:", _.map(races, "rid"));

    if (!_.isEmpty(rcr.fee_tag))
      races = races.filter((r) => rcr.fee_tag.includes(r.fee_tag));
    if (test_mode) console.log("a1:", _.map(races, "rid"));
    if (tdoc.type == "flash") {
      races = races.slice(0, 5) || [];
    }

    races = races.map((rrow) => {
      let score = calc_t_score(rrow, tdoc);
      rrow.score = score;
      return rrow;
    });

    races = _.sortBy(races, (r) => -nano(r.date));
    if (test_mode) console.log("a2:", _.map(races, "rid"));
    if (test_mode) console.table(races);

    let traces_n = races.length;
    let tot_score = _.sumBy(races, "score");
    if ([NaN, undefined, null].includes(tot_score)) tot_score = 0;
    let avg_score = (tot_score || 0) / (traces_n || 1);

    update_doc = {
      hid,
      traces_n,
      tot_score,
      avg_score,
      races,
      entry_date,
    };
  } else {
    upd = await elo_races_do(hid, tdoc, races);
    update_doc = { ...update_doc, ...upd };
    // if (update_doc?.traces_n > 0) console.log("ELO:: ", update_doc);
  }
  if (test_mode) console.log({ ...update_doc, races: "del" });
  return update_doc;
};

const run_t_give_ranks = (hdocs, tdoc) => {
  let mode = tdoc.score_mode;
  let type = tdoc.score_type;
  let k =
    (mode == "total" && "tot_score") ||
    (mode == "avg" && "avg_score") ||
    (mode == "elo" && "elo_score") ||
    null;
  let lim =
    (type == "regular" && mode == "elo" && 10) || (type == "flash" && 5) || 5;

  null;
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
    if (e[k] != 0 && e.traces_n >= lim) rank = ++i;
    return { ...e, rank };
  });
  if (test_mode) console.log(hdocs);
  return hdocs;
};

const run_t_tot_fees = async (tid, tdoc) => {
  let { tourney_st, tourney_ed, entry_st, entry_ed, type, status } = tdoc;
  let pays = await zed_db.db
    .collection("payments")
    .find(
      {
        status_code: 1,
        service: tcoll_stables(tid),
        ...(type == "flash" && status == "open"
          ? { date: { $gte: entry_st } }
          : { date: { $gte: entry_st, $lte: entry_ed } }),
        "meta_req.type": { $in: ["fee", "sponsor"] },
      },
      {
        projection: {
          pay_id: 1,
          req_amt: 1,
          "meta_req.type": 1,
          "meta_req.hids": 1,
          "meta_req.stable_name": 1,
        },
      }
    )
    .toArray();
  console.table(_.map(pays, (e) => ({ ...e, ...e.meta_req })));
  let tot_fees_act =
    _.chain(pays)
      .filter((i) => getv(i, "meta_req.type") == "fee")
      .map("req_amt")
      .filter((e) => ![null, undefined, NaN].includes(parseFloat(e)))
      .sum()
      .value() ?? 0;
  let tot_sponsors_act =
    _.chain(pays)
      .filter((i) => getv(i, "meta_req.type") == "sponsor")
      .map("req_amt")
      .filter((e) => ![null, undefined, NaN].includes(parseFloat(e)))
      .sum()
      .value() ?? 0;
  let hids = _.chain(pays)
    .filter((i) => getv(i, "meta_req.type") == "fee")
    .map("meta_req.hids")
    .value();
  hids = _.flatten(hids);
  let hidsC = _.countBy(hids, (e) => e);
  _.entries(hidsC).map(([hid, e]) => {
    if (e > 1) {
      console.log("dup", hid);
    }
  });
  hids = _.chain(hids).uniq().compact().value();

  let tot_fees = tdoc.entry_fee * hids.length;
  return { tot_fees, tot_fees_act, tot_sponsors_act, tot_score: 0 };
};

const fuser0_timer = 1 * 60;
const fuser1_timer = 1 * 60;
const fuser2_timer = 1 * 60;

const refund_pay_user = async ({ tid, stable_name, pay_id }) => {
  console.log("refunding user", stable_name, "at", tid);
  let tdoc = await zed_db.db
    .collection(tcoll)
    .findOne({ tid }, { projection: { _id: 0, tid: 1, type: 1 } });
  let txdoc = await zed_db.db.collection("payments").findOne({ pay_id });
  let { type } = tdoc;
  let { req_amt, sender, reciever } = txdoc;
  if (type == "flash") {
    payout_wallet = await refund_user({
      tid,
      wallet: sender,
      amt: req_amt,
      stable_name,
      payout_wallet: flash_payout_wallet,
    });
    let pays = [{ WALLET: sender, AMOUNT: req_amt.toString() }];
    if (payout_test == 0)
      await send_weth.sendAllTransactions(pays, flash_payout_private_key);
  } else if (type == "regular") {
  }
};

const process_t_status_flash = async ({ tid }) => {
  console.log("flash status tid");
  let upd = {};
  let tdoc = await get_tdoc(tid);
  let {
    type,
    status,
    flash_params,
    tourney_st,
    tourney_ed,
    entry_st,
    entry_ed,
  } = tdoc;
  console.log({ type, status });
  console.log({ entry_st, entry_ed });
  console.log({ tourney_st, tourney_ed });
  let now = moment().toISOString();
  // if (status == "ended") return {};
  if (tourney_ed < now) return { status: "ended" };

  if (entry_st > now) return { status: "upcoming" };

  if (status !== "open" && now > entry_st && !tourney_st && !tourney_ed) {
    upd.status = status = "open";
    return { status: "open" };
  }

  if (status == "open") {
    let hdocs = await zed_db.db
      .collection(tcoll_horses(tid))
      .find({}, { projection: { hid: 1, stable_name: 1 } })
      .toArray();
    hdocs = _.chain(hdocs)
      .groupBy("stable_name")
      .entries()
      .map(([k, e]) => [k, _.map(e, "hid")])
      .fromPairs()
      .value();
    let stables = _.keys(hdocs);
    let horses_n = _.flatten(_.values(hdocs))?.length || 0;
    console.log("stables", stables.length, "->", stables);

    if (horses_n >= tdoc.flash_params.minh) {
      return {
        status: "live",
        tourney_st: iso(),
        tourney_ed: moment()
          .add(tdoc.flash_params.duration, "hours")
          .toISOString(),
        entry_ed: iso(),
      };
    } else if (stables.length == 0) {
      let st = entry_st;
      let diff = moment(now).diff(moment(st), "minutes");
      console.log("stable0", diff, "minutes");
      return {};
    } else if (stables.length == 1) {
      let txdoc = await zed_db.db.collection("payments").findOne({
        date: { $gte: entry_st },
        service: tcoll_stables(tid),
        status_code: 1,
        "meta_req.stable_name": stables[0],
      });
      let st = txdoc.date;
      console.log("stable_1", stables[0], txdoc.pay_id, st);
      let diff = moment(now).diff(moment(st), "minutes");
      console.log("stable_1", diff, "minutes");
      if (diff >= fuser1_timer) {
        if (getv(tdoc, "terminated") !== true) {
          await zed_db.db
            .collection(tcoll)
            .updateOne({ tid }, { $set: { terminated: true } });
          await refund_pay_user({
            tid,
            stable_name: stables[0],
            pay_id: txdoc.pay_id,
          });
          let ed = moment(st).add(fuser1_timer, "minutes").toISOString();
          return {
            status: "ended",
            tourney_st: ed,
            tourney_ed: ed,
            entry_ed: ed,
          };
        }
      } else {
        return {
          status: "open",
          tourney_st: null,
          tourney_ed: null,
          entry_ed: moment(st).add(fuser1_timer, "minutes").toISOString(),
        };
      }
    } else if (stables.length >= 2) {
      let txdoc1 = await zed_db.db.collection("payments").findOne({
        date: { $gte: entry_st },
        service: tcoll_stables(tid),
        "meta_req.stable_name": stables[1],
        status_code: 1,
      });
      let txdoc2 = await zed_db.db.collection("payments").findOne({
        date: { $gte: entry_st },
        service: tcoll_stables(tid),
        "meta_req.stable_name": stables[0],
        status_code: 1,
      });
      let txdoc = nano(txdoc2.date) > nano(txdoc1.date) ? txdoc2 : txdoc1;
      let st = txdoc.date;
      let diff = moment(now).diff(moment(st), "minutes");
      console.log("stable_2", diff, "minutes");
      if (diff >= fuser2_timer)
        return {
          status: "live",
          tourney_st: iso(),
          tourney_ed: moment()
            .add(tdoc.flash_params.duration, "hours")
            .toISOString(),
          entry_ed: iso(),
        };
      else {
        return {
          status: "open",
          tourney_st: null,
          tourney_ed: null,
          entry_ed: moment(st).add(fuser2_timer, "minutes").toISOString(),
        };
      }
    }
  }
  if (status == "live" && tourney_st && tourney_ed) {
    if (!_.inRange(nano(now), nano(tourney_st), nano(tourney_ed)))
      return { status: "ended" };
    else return { status: "live" };

    return upd;
  }
};

const t_status_flash = async () => {
  let docs = await zed_db.db
    .collection(tcoll)
    .find(
      {
        $or: [
          {
            type: { $eq: "flash" },
            tourney_ed: { $gte: moment().add(20, "minutes").toISOString() },
          },
          {
            type: { $eq: "flash" },
            tourney_ed: { $eq: null },
          },
        ],
      },
      { projection: { _id: 0, tid: 1 } }
    )
    .toArray();
  let tids = _.map(docs, "tid");
  for (let tid of tids) {
    let upd = await process_t_status_flash({ tid });
    console.log("UPDATE STATUS", upd);
    if (!_.isEmpty(upd))
      await zed_db.db.collection(tcoll).updateOne({ tid }, { $set: upd });
    console.log("===");
  }
};

// upcoming => entry not started
// open => entry started
// live => tourney started running
// ended => tourney ended
const t_status_regular = async () => {
  let docs = await zed_db.db
    .collection(tcoll)
    .find(
      {
        $or: [
          {
            type: { $eq: "regular" },
            tourney_ed: { $gte: moment().add(-1, "days").toISOString() },
          },
          { type: { $eq: "regular" }, tourney_ed: { $eq: null } },
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
  // console.table(ar);
  await bulk.push_bulkc(tcoll, ar, "tourney:t_status_regular", "tid");
};

const run_tid = async (tid) => {
  let tdoc = await get_tdoc(tid, {});
  // console.log(tdoc);
  if (!tdoc) return console.log(`[ ${tid} ] no such tourney`);
  if (!eval_hidden && tdoc.hide == true)
    return console.log(`[ ${tid} ] this tourney is hidden`);
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

  let entry_fee = 0;
  console.log("tdoc.horse_cr.length", tdoc.horse_cr.length);
  if (tdoc.horse_cr.length > 1) entry_fee = "multi";
  else entry_fee = getv(tdoc, "horse_cr.0.cost") ?? 0;
  tdoc.entry_fee = entry_fee;

  let txns_paid = _.filter(txns, (i) => i.status_code == 1);
  let pays_doc = await run_t_tot_fees(tid, tdoc);
  let total_capital = pays_doc.tot_fees;
  let prize_pool = parseFloat(utils.dec(total_capital * 0.95, 4));

  let prize_pool_usd = eth_t_usd(prize_pool);
  let entry_fee_usd = entry_fee == "multi" ? "multi" : eth_t_usd(entry_fee);
  let ft = get_ft(
    entry_fee_usd == "multi" ? getv(tdoc, "horse_cr.0.cost") : entry_fee_usd
  );
  let fins = {
    ...pays_doc,
    total_capital,
    prize_pool,
    entry_fee,
    prize_pool_usd,
    entry_fee_usd,
    ft,
  };
  if (fins.tot_fees == -1) {
    fins.total_capital = -1;
    fins.prize_pool = -1;
    fins.prize_pool_usd = -1;
  }
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
    let upd = await process_t_status_flash({ tid });
    console.log("UPDATE STATUS", upd);
    if (!_.isEmpty(upd)) {
      await zed_db.db.collection(tcoll).updateOne({ tid }, { $set: upd });
      await cdelay(1000);
    }
    tdoc = await get_tdoc(tid);
    // console.log(tdoc);
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
  // if (test_mode) 
  console.table(update_ar);
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
const run_flash_cron = async () => {
  console.log("##run flash cron", "tourney");
  print_cron_details(cron_str);
  cron.schedule(cron_str, flash_runner, cron_conf);
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
  await update_eth();
  await t_status_regular();
  await run_tid(tid);
};

const runtcron = async (...a) => {
  let running = 0;
  let cron_str = "*/20 * * * * *";
  print_cron_details(cron_str);
  const runner = async () => {
    if (running == 1) return;
    try {
      running = 1;
      // await zed_db.db
      //   .collection(tcoll_horses(a[0]))
      //   .updateMany({}, { $set: { rank: null } });
      await run(...a);
    } catch (err) {
      console.log(err);
    }
    running = 0;
  };
  await runner();
  cron.schedule(cron_str, runner);
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

const get_ranked_leader_t = async ({ tid }) => {
  let tdoc = (await zed_db.db.collection(tcoll).findOne({ tid })) || null;
  if (!tdoc) throw new Error("now such tourney");
  let { prize_pool, payout_mode, score_mode, status, terminated } = tdoc;
  // prize_pool = 1;
  // console.log({ prize_pool, payout_mode, score_mode });
  let k =
    (score_mode == "total" && "tot_score") ||
    (score_mode == "avg" && "avg_score") ||
    null;
  let leader = await get_leaderboard_t({ tid });
  if (leader.length == 0) return [];
  if (status == "open") return leader;
  if (terminated == true) return leader;

  let pays = [];

  if (payout_mode == "winner_all") {
    pays = get_winner_all_list(tdoc, leader);
    // console.table(leader);
    // if (!leader[0].rank) pays = [];
    // else pays = [{ ...leader[0], amt: prize_pool, role: "winner_all" }];
  } else if (payout_mode == "double_up") {
    pays = get_double_up_list(tdoc, leader);
    // console.table(pays);
  }
  pays = _.keyBy(pays, "hid");
  leader = leader.map((l) => {
    let ob = pays[l.hid] || { role: "lose", amt: 0 };
    return { ...l, ...ob };
  });
  return leader;
};
const calc_payouts_list = async ({ tid }) => {
  let leader = await get_ranked_leader_t({ tid });
  if (_.isEmpty(leader)) return [];
  leader = _.filter(leader, (e) => ![undefined, NaN, null, 0].includes(e.amt));
  return leader;
};

const flash_payout = async (tid) => {
  console.log("flash_payout");
  console.log(`## [ ${tid} ] started payout`, iso());
  let pays = await calc_payouts_list({ tid });
  let tdoc = await get_tdoc(tid);
  let is_override = process.argv.includes("override") ?? false;
  if (tdoc.payout_done == true && !is_override)
    return console.log("payout done at", tdoc.payout_date);
  if (tdoc.status !== "ended") return console.log("tourney not ended");
  let eda = moment().add(-5, "minutes").toISOString();
  let edb = moment().add(-15, "minutes").toISOString();
  eda = nano(eda);
  edb = nano(edb);
  if (nano(tdoc.tourney_ed) > eda)
    return console.log("wait a while before paying this out");
  if (getv(tdoc, "terminated") === true) {
    return console.log("this tourney was terminated and refunded");
  }
  if (_.isEmpty(pays)) {
    console.log("nothing to payout");
  } else {
    console.table(pays);
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
        status: { $in: ["open"] },
      },
      { projection: { tid: 1, _id: 0, created_using: 1 } }
    )
    .toArray();
  let pids_present =
    _.chain(tids_doc).map("created_using").uniq().value() || [];
  console.log("pids present", pids_present.length, pids_present);

  let pids_consider = _.difference(pids_active, pids_present);
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
        console.log("flash_payout", tid);
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
  await update_eth();
  if (frunning) {
    console.log("############# tourney already frunning.........");
    return;
  }
  try {
    frunning = 1;
    await t_status_flash();
    await flash_run_current();
    await flash_payout_ended();
    await flash_auto_start();
    await t_status_flash();
    frunning = 0;
  } catch (err) {
    console.log("TOURNEY ERR\n", err);
    frunning = 0;
  }
};

const runner = async () => {
  await update_eth();
  if (running) {
    console.log("############# tourney already running.........");
    return;
  }
  try {
    running = 1;
    await t_status_regular();
    await regular_runner();
    // await flash_runner();
    await t_status_regular();
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
    if (arg2 == "test") await test(arg3);
    if (arg2 == "thorse") await thorse([arg3, arg4]);
    if (arg2 == "run") await run(arg3);
    if (arg2 == "runtcron") await runtcron(arg3);
    if (arg2 == "runner") await runner();
    if (arg2 == "flash_runner") await flash_runner();
    if (arg2 == "flash_payout") await flash_payout(arg3);
    if (arg2 == "run_cron") await run_cron();
    if (arg2 == "run_flash_cron") await run_flash_cron();
    if (arg2 == "t_status_regular") await t_status_regular();
  } catch (err) {
    console.log("Err in tourney runner", err);
  }
};

const test = async () => {
  const cron_str = "*/20 * * * * *";
  const fn = async () => {
    let tid = "ae0877c0";
    await run_tid(tid);
    await flash_payout(tid);
  };
  console.log("##run test cron", "tourney");
  print_cron_details(cron_str);
  cron.schedule(cron_str, fn, cron_conf);
};

const tourney = {
  test,
  thorse,
  runner,
  flash_runner,
  run,
  run_cron,
  main_runner,
  t_status_regular,
};

module.exports = tourney;
