const _ = require("lodash");
const moment = require("moment");
const { zed_db, zed_ch } = require("../connection/mongo_connect");
const crypto = require("crypto");
const utils = require("../utils/utils");

const tcoll = "tourney_master";
const tcollp = "tourney_preset";

const tcoll_horses = (tid) => `tourney::${tid}::horses`;
const tcoll_stables = (tid) => `tourney::${tid}::stables`;

const tid_len = 8;

const danshan_eth_address = process.env.danshan_eth_address;

const ADMIN_KEY = process.env.ADMIN_KEY;
const is_admin = (k) => k == ADMIN_KEY;

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
    let hdata = hdocs[h.hid];
    return { ...h, ...hdata };
  });
  ar = _.sortBy(ar, (i) => i.rank || 1e14);
  return ar;
};

const payout_list = async (body) => {
  let { tid } = body;
  if (!tid) throw new Error("tid not found");
  let leader = await get_leaderboard_t({ tid });
  let stables = await zed_db.db
    .collection(tcoll_stables(tid))
    .find({}, { projection: { discord: 1, _id: 0, wallet: 1, stable_name: 1 } })
    .toArray();
  // console.log(stables);
  stables = _.chain(stables).keyBy("stable_name").value();

  leader = leader.map((l) => {
    let { stable_name } = l;
    let sdoc = stables[stable_name] || {};
    return { ...l, ...sdoc };
  });
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
};
