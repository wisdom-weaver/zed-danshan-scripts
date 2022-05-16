const _ = require("lodash");
const moment = require("moment");
const { zed_db, zed_ch } = require("../connection/mongo_connect");
const crypto = require("crypto");
const utils = require("../utils/utils");
const { getv } = require("../utils/utils");
const send_weth = require("../payments/send_weth");
const sha256 = require("crypto-js/sha256");
const zedf = require("../utils/zedf");

require("dotenv").config();

const tcoll = "tourney_master";
const tcollp = "tourney_preset";

const tcoll_horses = (tid) => `tourney::${tid}::horses`;
const tcoll_stables = (tid) => `tourney::${tid}::stables`;

const tid_len = 8;

const ADMIN_KEY = process.env.ADMIN_KEY;
const is_admin = (k) => k == ADMIN_KEY;

const flash_payout_wallet = process.env.flash_payout_wallet;
const flash_payout_private_key = process.env.flash_payout_private_key;
const danshan_eth_address = process.env.danshan_eth_address;

const horse_pro = {
  _id: 0,
  hid: 1,
  name: 1,
  tc: 1,
  bloodline: 1,
  breed_type: 1,
  horse_type: 1,
  genotype: 1,
  color: 1,
  hex_code: 1,
};

const get_hdata = async (hid) => {
  let doc = await zed_db.db
    .collection("horse_details")
    .findOne({ hid }, { projection: horse_pro });
  if (doc) return doc;
  return {
    hid,
    name: "Horse Name",
    tc: 6,
    bloodline: "na",
    breed_type: "na",
    horse_type: "na",
    genotype: "na",
    color: "NA",
    hex_code: "FFFFFF",
  };
};

const get_leaderboard_t = async ({ tid }) => {
  if (!tid) throw new Error("tid not found");
  let stables = await zed_db.db
    .collection(tcoll_stables(tid))
    .find({}, { projection: { discord: 1, _id: 0, wallet: 1, stable_name: 1 } })
    .toArray();
  stables = _.chain(stables).keyBy("stable_name").value();
  let docs = await zed_db.db
    .collection(tcoll_horses(tid))
    .find(
      {},
      {
        projection: {
          _id: 0,
          hid: 1,
          rank: 1,
          stable_name: 1,
          tot_score: 1,
          avg_score: 1,
          elo_score: 1,
          traces_n: 1,
        },
      }
    )
    .sort({ rank: 1 })
    .toArray();
  if (_.isEmpty(docs)) {
    return [];
  }
  let hids = _.map(docs, "hid");
  let hdocs = await Promise.all(hids.map((hid) => get_hdata(hid)));
  hdocs = _.keyBy(hdocs, "hid");
  let ar = docs.map((h) => {
    let sdoc = stables[h.stable_name] || {};
    let hdata = hdocs[h.hid];
    return { ...h, ...hdata, ...sdoc };
  });
  ar = _.sortBy(ar, (i) => i.rank || 1e14);
  return ar;
};

const payout_list = async (body) => {
  let { tid } = body;
  if (!tid) throw new Error("tid not found");
  let leader = await get_leaderboard_t({ tid });
  let mid = parseInt(leader.length / 2);
  leader = leader.slice(0, mid);
  return leader;
};

const get_p = async (body) => {
  let { pid } = body;
  if (!pid) throw new Error("pid not found");
  let doc = await zed_db.db.collection(tcollp).findOne({ pid });
  if (!doc) throw new Error("no such preset found");
  return doc;
};

const generate_tid = (title, tag, create_date) => {
  let comb_str = `${title}||${tag}||${create_date}`;
  const hash = crypto
    .createHash("sha256", ADMIN_KEY)
    .update(comb_str)
    .digest("hex");
  return hash.slice(-tid_len);
};

const process_tdoc = (body) => {
  let {
    tid,
    type,
    tag,
    flash_params,
    title,
    tourney_st,
    tourney_ed,
    entry_st,
    entry_ed,
    horse_cr,
    race_cr,
    score_cr,
    rules,
    logo,
    score_mode,
    payout_mode,
    created_using = undefined,
  } = body;
  if (!tid) throw new Error("tid not found");

  if (!title) throw new Error("title not found");
  if (!logo) throw new Error("add tourney logo image");
  if (!entry_st) throw new Error("entry_st not found");

  if (!type) type = "regular";
  if (type !== "flash") {
    flash_params = undefined;
  } else {
    flash_params.minh = parseInt(flash_params.minh);
    flash_params.duration = parseFloat(flash_params.duration);
    tourney_ed = null;
    entry_ed = null;
    tourney_st = null;
  }
  if (type !== "flash") {
    if (!tourney_st) throw new Error("tourney_st not found");
    if (!tourney_ed) throw new Error("tourney_ed not found");
    if (!entry_ed) throw new Error("entry_ed not found");
  }

  if (!horse_cr) throw new Error("horse_cr not found");
  if (!race_cr) throw new Error("race_cr not found");
  if (!score_cr) throw new Error("score_cr not found");
  if (!rules) throw new Error("rules not found");
  let doc = {
    tid,
    type,
    tag,
    title,
    tourney_st,
    tourney_ed,
    entry_st,
    entry_ed,
    logo,
    horse_cr,
    race_cr,
    score_cr,
    score_mode,
    payout_mode,
    rules,
    flash_params,
    created_using,
  };
  return doc;
};

const create_t = async (body) => {
  // console.log(ob);
  let create_date = utils.iso();
  let { tag, title = "" } = body;
  let tid = generate_tid(title, tag, create_date);
  if (!tid) throw new Error("tid not found");
  let exis = (await zed_db.db.collection(tcoll).findOne({ tid: tid })) || null;
  if (exis) throw new Error(`Tourney with tid: ${tid} exists`);
  body.tid = tid;
  let doc = process_tdoc(body);
  let resp = (await zed_db.db.collection(tcoll).insertOne(doc)) || {};
  if (!(resp.insertedCount > 0)) throw new Error("couldnt create tourney");

  resp = { status: "success", msg: `Tourney Created with id: ${tid}`, tid };
  // console.log(resp);
  return resp;
};

const add_transaction = async (req) => {
  let { reciever, sender, req_amt = null, token, service, meta_req = {} } = req;
  let date = utils.iso();
  if (!sender) throw new Error("sender address not found");
  if (!reciever) throw new Error("sender address not found");
  sender = sender.toLowerCase();
  if (!service) throw new Error("service not found");
  if (!token) throw new Error("token not found");
  const unq = `${sender}-${date}-${reciever}-${req_amt}-${service}`;
  const hash = crypto.createHmac("sha256", unq).digest("hex");
  // console.log(date);
  // console.log(unq);
  // console.log(hash);

  const pay_id = hash.slice(hash.length - 8);
  let doc = {
    pay_id,
    sender,
    reciever,
    req_amt,
    token,
    service,
    status: "pending",
    status_code: 0,
    date,
    meta_req,
    meta: {},
  };
  let resp = await zed_db.db.collection("payments").insertOne(doc);
  if (resp.insertedCount) return { status: "success", pay_id };
  else throw new Error("couldnt add your payment");
};

const payout_single = async ({
  tid,
  wallet,
  amt,
  stable_name,
  payout_wallet,
}) => {
  const pay_body = {
    sender: payout_wallet.toLowerCase(),
    reciever: wallet,
    req_amt: amt,
    token: "WETH",
    service: tcoll_stables(tid),
    meta_req: {
      service_cat: "tourney",
      type: "payout",
      tid,
      stable_name,
    },
  };
  // console.table([pay_body])
  let resp = await add_transaction(pay_body);
  await zed_db.db
    .collection(tcoll_stables(tid))
    .updateOne({ stable_name }, { $addToSet: { transactions: resp.pay_id } });

  return resp.pay_id;
};
const refund_user = async ({
  tid,
  wallet,
  amt,
  stable_name,
  payout_wallet,
}) => {
  const pay_body = {
    sender: payout_wallet.toLowerCase(),
    reciever: wallet,
    req_amt: amt,
    token: "WETH",
    service: tcoll_stables(tid),
    meta_req: {
      service_cat: "tourney",
      type: "refund",
      tid,
      stable_name,
    },
  };
  let resp = await add_transaction(pay_body);
  await zed_db.db
    .collection(tcoll_stables(tid))
    .updateOne({ stable_name }, { $addToSet: { transactions: resp.pay_id } });

  return resp.pay_id;
};

const payout_all = async (body) => {
  let { tid, amt } = body;
  if (!tid) throw new Error("tid not found");
  amt = parseFloat(amt);
  if ([0, null, undefined, NaN].includes(amt))
    throw new Error("invalid amount");
  let list = await payout_list({ tid });
  // console.log(list);
  await Promise.all(
    list.map((l) =>
      payout_single({
        tid,
        stable_name: l.stable_name,
        wallet: l.wallet,
        amt,
      })
    )
  );
  const csv = create_payout_csv(list, amt);
  // console.log(csv);
  let filename = `${tid}-payout_all-${utils.iso()}.csv`;
  return {
    status: "success",
    filename,
    csv,
    msg: "payouts set... please copy the csv data",
  };
};

const get_double_up_list = (tdoc, leader) => {
  let { tid, prize_pool, payout_mode, score_mode, horse_cr } = tdoc;
  let entry_fee = getv(horse_cr, "0.cost");
  let k =
    (score_mode == "total" && "tot_score") ||
    (score_mode == "avg" && "avg_score") ||
    (score_mode == "elo" && "elo_score") ||
    null;

  let tot = leader.length;
  let is_even = tot % 2 == 0;
  let mid = parseInt(leader.length / 2);
  let win_n = mid;
  let entry_n = is_even ? 0 : 1;
  let entry_pot = entry_n * entry_fee;
  let win_pot = prize_pool - entry_pot;
  let entry_spot = entry_pot * 1;
  let win_spot = win_pot / mid;
  // console.log({
  //   tid: tdoc.tid,
  //   prize_pool,
  //   payout_mode,
  //   score_mode,
  //   entry_fee,
  //   tot,
  //   is_even,
  //   mid,
  //   win_n,
  //   entry_n,
  //   entry_pot,
  //   win_pot,
  //   entry_spot,
  //   win_spot,
  // });
  if (leader.length == 1) return leader;

  let above_line = leader[mid - 1];
  let below_line = leader[mid];
  let pays = [];
  if (is_even) {
    let win_ties = _.filter(leader, (i) => i[k] == above_line[k]);
    if (win_ties.length == 0 || win_ties.length == 1) win_ties = [];
    let win_ties_hids = _.map(win_ties, "hid");
    let wins = _.filter(
      leader.slice(0, mid),
      (i) => !win_ties_hids.includes(i.hid)
    );
    wins.map((e) => {
      if (!e.amt) e.amt = 0;
      e.amt += win_spot;
      if (!e.role) e.role = "";
      e.role += "|win";
    });

    let win_tie_amt = (win_pot - _.sumBy(wins, "amt")) / win_ties.length;
    // console.log({ win_tie_amt });

    win_ties.map((e) => {
      if (!e.amt) e.amt = 0;
      e.amt += win_tie_amt;
      if (!e.role) e.role = "";
      e.role += "|win_ties";
    });

    pays = [...(wins || []), ...(win_ties || [])];

    pays = _.uniqBy(pays, "hid");
    pays = pays.map((e) => {
      let { rank, hid, wallet, amt, stable_name } = e;
      return { rank, hid, val: e[k], wallet, amt, stable_name };
    });
    console.table(pays);
    let total = _.sumBy(pays, "amt");
    // console.log({ total });
  } else {
    let win_ties = _.filter(leader, (i) => i[k] == above_line[k]);
    if (win_ties.length == 0 || win_ties.length == 1) win_ties = [];
    let win_ties_hids = _.map(win_ties, "hid");
    let entry_ties = _.filter(leader, (i) => i[k] == below_line[k]);
    let wins = _.filter(
      leader.slice(0, mid),
      (i) => !win_ties_hids.includes(i.hid)
    );
    wins.map((e) => {
      if (!e.amt) e.amt = 0;
      e.amt += win_spot;
      if (!e.role) e.role = "";
      e.role += "|win";
      // e.win = win_spot;
    });

    let win_tie_amt = (win_pot - _.sumBy(wins, "amt")) / win_ties.length;
    let entry_tie_amt = entry_spot / entry_ties.length;
    // console.log({ win_tie_amt, entry_tie_amt });

    win_ties.map((e) => {
      if (!e.amt) e.amt = 0;
      e.amt += win_tie_amt;
      if (!e.role) e.role = "";
      e.role += "|win_ties";
      // e.win_tie = win_tie_amt;
    });
    entry_ties.map((e) => {
      if (!e.amt) e.amt = 0;
      e.amt += entry_tie_amt;
      if (!e.role) e.role = "";
      e.role += "|entry_ties";
      // e.entry_tie = entry_tie_amt;
    });

    pays = [...(wins || []), ...(win_ties || []), ...(entry_ties || [])];
    pays = _.uniqBy(pays, "hid");
    pays = pays.map((e, idx) => {
      let { rank, hid, wallet, amt, role } = e;
      if (idx == mid) role += "|mid";
      return { rank, hid, val: e[k], wallet, amt, role };
    });
    let total = _.sumBy(pays, "amt");
    // console.log({ total });
  }
  pays = _.map(pays, (e) => {
    let amt = amt_manip_leader(e, tid);
    return { ...e, amt };
  });
  return pays;
};

const get_winner_all_list = (tdoc, leader) => {
  let { tid, prize_pool, payout_mode, score_mode, horse_cr } = tdoc;
  let entry_fee = getv(horse_cr, "0.cost");
  let k =
    (score_mode == "total" && "tot_score") ||
    (score_mode == "avg" && "avg_score") ||
    (score_mode == "elo" && "elo_score") ||
    null;

  let tot = leader.length;
  let win_pot = prize_pool;

  let win = leader[0];
  if (!win?.rank) return [];

  // console.log(win)

  let win_ties = _.filter(leader, (i) => i[k] == win[k]);
  if (win_ties.length == 1) {
    return [{ ...win, amt: prize_pool, role: "winner_all" }];
  }

  let win_tie_amt = win_pot / win_ties.length;

  win_ties.map((e) => {
    if (!e.amt) e.amt = 0;
    e.amt += win_tie_amt;
    e.win_tie = win_tie_amt;
    e.role = "winner_all";
  });
  // console.table(win_ties)

  let pays = win_ties || [];
  pays = _.uniqBy(pays, "hid");
  pays = pays.map((e) => {
    let { rank, hid, wallet, amt, role } = e;
    return { rank, hid, val: e[k], wallet, amt, role };
  });

  pays = _.map(pays, (e) => {
    let amt = amt_manip_leader(e, tid);
    return { ...e, amt };
  });

  return pays;
};

const flash_pay_to_user = async (pays) => {
  let payments = pays.map((l) => ({
    WALLET: l.wallet,
    AMOUNT: l.amt.toString(),
  }));
  console.table(payments);
  let count = await send_weth.sendAllTransactions(
    payments,
    flash_payout_private_key
  );
  console.log("done txns:", count);
};

const amt_manip_leader = (hrow, tid) => {
  let { hid, wallet, amt } = hrow;
  let base = amt;
  const unq = `${tid}-${wallet}-${hid}`;
  let hash = sha256(unq).toString();
  let fact = parseInt(Math.log10(base));
  let add = parseInt(hash.slice(-6), "16") % 1000;
  add = add * 1e-8;
  base = base.toString();
  base = parseFloat(base.slice(0, base.indexOf(".") + 5));
  let tot = base + add;
  tot = parseFloat(tot).toFixed(8);
  // console.log({ base, add, tot });
  return tot;
};

const get_elo_score = async (hid) => {
  let hdoc = await zedf.horse(hid);
  return hdoc?.rating || null;
};

module.exports = {
  tcoll,
  tcollp,
  tcoll_horses,
  tcoll_stables,
  payout_list,
  get_leaderboard_t,
  get_p,
  create_t,
  payout_single,
  get_double_up_list,
  get_winner_all_list,
  refund_user,
  flash_payout_wallet,
  flash_payout_private_key,
  danshan_eth_address,
  flash_pay_to_user,
  get_elo_score,
};
