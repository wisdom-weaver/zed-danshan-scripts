const _ = require("lodash");
const { zed_db } = require("../connection/mongo_connect");
const cyclic_depedency = require("../utils/cyclic_dependency");
const utils = require("../utils/utils");
const etherscan = require("./etherscan");
const polygonscan = require("./polygonscan");
const moment = require("moment");
const cron = require("node-cron");
const crypto = require("crypto");
const { iso, getv, nano, delay } = require("../utils/utils");
const sheet_ops = require("../../sheet_ops/sheets_ops");
const { update } = require("lodash");
const { push_bulkc } = require("../utils/bulk");

const danshan_eth_address = process.env.danshan_eth_address;

const allowed_buffer = 15 * utils.mt;
const mimi = 100;
const coll = "payments";

const tokens_ob = {
  MATIC: {
    token: "MATIC",
    get_balance: polygonscan.get_matic_balance,
    get_tx_status: polygonscan.get_tx_status,
    get_txs: polygonscan.get_matic_txs,
  },
  ETH: {
    token: "ETH",
    get_balance: etherscan.get_eth_balance,
    get_tx_status: etherscan.get_tx_status,
    get_txs: etherscan.get_eth_txs,
  },
  WETH: {
    token: "WETH",
    get_balance: polygonscan.get_weth_balance,
    get_tx_status: polygonscan.get_tx_status,
    get_txs: polygonscan.get_weth_txs,
  },
};

let running = 0;

const get_payments_list = async ({
  before,
  after,
  token,
  status_code,
  status_codes,
}) => {
  let query = {};
  if (token) query.token = token;
  if (![null, undefined, NaN].includes(status_code))
    query.status_code = { $in: [status_code] };
  if (![null, undefined, NaN].includes(status_codes))
    query.status_code = { $in: status_codes };
  if (after || before) query.date = {};
  if (after) query.date.$gte = utils.iso(after);
  if (before) query.date.$lte = utils.iso(before);
  console.log(query);
  let ar = await zed_db.db.collection(coll).find(query).toArray();
  return ar;
};
const get_if_existing_txns = async (txids = []) => {
  if (txids.length == 0) return [];
  let ar =
    (await zed_db.db
      .collection(coll)
      .find(
        { "meta.tx.hash": { $in: txids } },
        { projection: { pay_id: 1, _id: 0, "meta.tx.hash": 1 } }
      )
      .toArray()) ?? [];
  let ids = _.map(ar, "meta.tx.hash");
  return ids;
};

const dummy_tx = ({ sender, reciever, req_amt, token, date }) => {
  const unq = `${sender}-${date}`;
  let hash = crypto.createHmac("sha256", unq).digest("hex");
  hash += "dummy";
  return {
    blockNumber: "25473802",
    timeStamp: utils.nano(date) / 1000,
    hash,
    nonce: "1548258",
    blockHash:
      "0xc7f0ea2ba55a6fd65feec8f952cd3bf47696cbb7228a710677178573e1_dummy",
    from: sender,
    contractAddress: "0x7ceb23fd6bc0add59e62ac25578270cff1b9f619",
    to: reciever,
    value: parseFloat(req_amt * 1e18).toFixed(0),
    tokenName: "dumm",
    tokenSymbol: token,
    tokenDecimal: "18",
    transactionIndex: "116",
    gas: "111110",
    gasPrice: "45600000000",
    gasUsed: "74073",
    cumulativeGasUsed: "15460844",
    input: "deprecated",
    confirmations: "566",
  };
};
const handle_dummies = async (dummies) => {
  if (_.isEmpty(dummies)) return [];
  console.log(_.map(dummies, "pay_id"));
  // let updates = dummies.map((i) => {
  //   return {
  //     pay_id: i.pay_id,
  //     meta: { tx: dummy_tx(i) },
  //     status_code: 1,
  //     status: "paid",
  //   };
  // });
  // await push_bulk(coll, updates, "update_dummies");
  return dummies.map(dummy_tx);
};

const verify_user_payments = async (
  [st, ed],
  status_codes = [0, -1],
  cron_mode = 1
) => {
  let update_ar = [];
  let update_far = [];
  const token = "WETH";
  let list = await get_payments_list({
    token,
    before: ed,
    after: st,
    status_codes,
  });
  if (_.isEmpty(list)) return console.log("nothing to do");
  console.table(list);

  let rx_list = _.map(list, "reciever");
  rx_list = _.uniq(rx_list);
  console.log(rx_list);

  let sx_list = _.map(list, "sender");
  sx_list = _.uniq(sx_list);
  console.log(sx_list);
  let txs = [];
  for (let rx of rx_list) {
    let txar = await tokens_ob[token].get_txs({ address: rx });
    txs.push(txar.result);
    console.log("txar.result.len: ", txar.result.length);
    await delay(500);
  }
  txs = _.flatten(txs);
  // console.log(txs);

  let txshash = _.map(txs, "hash");
  // console.log(txshash);
  let exists_tx = await zed_db.db
    .collection("payments")
    .find(
      {
        $or: [
          { date: { $gte: st, $lte: ed }, status_code: 1 },
          { "meta.tx.hash": { $in: txshash } },
        ],
      },
      { projection: { pay_id: 1, "meta.tx.hash": 1 } }
    )
    .toArray();
  exists_tx = _.map(exists_tx, (e) => {
    return { pay_id: e.pay_id, hash: getv(e, "meta.tx.hash") };
  });
  // console.table(exists_tx);
  console.log("txns.len", txs.length);
  console.log("exists_tx.len", exists_tx.length);

  const list_map = _.chain(list)
    .groupBy("sender")
    .entries()
    .map(([sender, sx_reqs]) => {
      sender = sender?.toLowerCase();
      sx_reqs = _.groupBy(sx_reqs, (e) => e["reciever"]?.toLowerCase());
      return [sender, sx_reqs];
    })
    .fromPairs()
    .value();
  console.log(list_map);

  for (let tx of txs) {
    let { from, to, hash } = tx;
    let hash_present = _.find(exists_tx, (e) => e.hash == hash);
    if (hash_present) continue;

    from = from.toLowerCase();
    to = to.toLowerCase();
    const shortlist = getv(list_map, `${from}.${to}`);
    if (_.isEmpty(shortlist)) continue;

    let { value, timeStamp } = tx;
    let tx_amt = parseFloat(value);
    let tx_nano = timeStamp * 1000;

    for (let s_req of shortlist) {
      let { pay_id, date, req_amt } = s_req;
      let hash_present = _.find(
        exists_tx,
        (e) => e.hash == hash || e.pay_id == pay_id
      );
      if (hash_present) continue;

      req_amt *= 1e18;
      let [req_amt_mi, req_amt_mx] = [req_amt - mimi, req_amt + mimi];
      if (_.isNaN(req_amt)) continue;
      if (!(req_amt == tx_amt || _.inRange(tx_amt, req_amt_mi, req_amt_mx))) {
        continue;
      }
      // console.log("amt match", tx_amt, req_amt, req_amt_mi, req_amt_mx);

      let req_nano = nano(date);
      let [req_nano_mi, req_nano_mx] = [
        req_nano - allowed_buffer,
        req_nano + allowed_buffer,
      ];
      if (!_.inRange(tx_nano, req_nano_mi, req_nano_mx)) {
        continue;
      }
      // console.log("date match", iso(tx_nano), iso(req_nano));
      console.log("pay", pay_id, "confimed paid");
      exists_tx.push({ pay_id, hash });
      update_ar.push({ pay_id, status: "paid", status_code: 1, meta: { tx } });
      break;
    }
  }
  console.log("# PAID confirmed", update_ar.length);
  await push_bulkc("payments", update_ar, "payments", "pay_id");

  const paid_pays = _.map(exists_tx, "pay_id");
  update_far = _.chain(list)
    .filter((e) => !paid_pays.includes(e.pay_id))
    .map((e) => {
      let date = e.date;
      let req_nano = nano(date);
      let req_nano_mx = req_nano + allowed_buffer;
      if (nano() > req_nano_mx)
        return { pay_id: e.pay_id, status_code: -1, status: "failed" };
      else return null;
    })
    .compact()
    .value();
  console.log("# FAILED timed", update_far.length);
  await push_bulkc("payments", update_far, "payments", "pay_id");
};

const verify_user_payments_old = async ([st, ed]) => {
  console.log("verify_user_payments\n", st, "->", ed);
  for (let { token, get_balance, get_txs } of _.values({
    WETH: tokens_ob["WETH"],
  })) {
    console.log("\n------------\n");
    console.log("started", utils.iso());
    console.log("token:", token);
    let list = await get_payments_list({
      token,
      before: ed,
      after: st,
      status_codes: [0, -1, 1],
    });

    let update_paid = [];

    console.log("pending:", list.length);
    console.table(list);
    if (list.length == 0) continue;
    // list = _.keyBy(list, "pay_id");
    let recievers = _.uniq(_.map(list, "reciever"));
    let list_gp = _.groupBy(list, "sender");
    console.log("recievers", recievers);
    let all_txns = [];
    for (let reciever of recievers) {
      let txns = await get_txs({ address: reciever });
      // console.table(txns)
      if (txns) txns = txns?.result || [];
      console.log("txns:", txns.length, "for", reciever);
      all_txns = [...all_txns, ...txns];
    }
    let dummies = _.filter(
      list,
      (i) => utils.getv(i, "meta_req.is_dummy") == true
    );
    dummies = await handle_dummies(dummies);

    console.log("dummies:", dummies.length);
    console.log("all_txns:", all_txns.length);
    console.table(all_txns);

    all_txns = [...all_txns, ...dummies];
    console.log("all_txns[updated]:", all_txns.length);

    let existing_txns = []; //await get_if_existing_txns(_.map(all_txns, "hash"));
    console.log("existing_txns:", existing_txns.length);

    for (let tx of all_txns) {
      if (existing_txns.includes(tx.hash)) {
        // console.log("tx exists already");
        continue;
      }
      let amt = parseFloat(tx.value);
      let timeStamp = parseFloat(tx.timeStamp) * 1000;

      for (let [sender, sender_reqs] of _.entries(list_gp)) {
        if (tx.from.toLowerCase() !== sender.toLowerCase()) {
          continue;
        }
        console.log(`https://polygonscan.com/tx/${tx.hash}`);
        console.log(tx.from, "->", tx.to);
        console.log("\n=====\nsender", sender, "=>");
        let got_request = _.find(sender_reqs, (req) => {
          console.log("check", req.sender, "->", req.reciever);
          if (!(req?.sender?.toLowerCase() == tx.from.toLowerCase())) {
            console.log("na sender");
            return false;
          }

          if (!(req?.reciever?.toLowerCase() == tx.to.toLowerCase())) {
            console.log("na reciever");
            return false;
          }

          let tnano = utils.nano(req.date);
          let [mi, mx] = [tnano - allowed_buffer, tnano + allowed_buffer];
          if (!_.inRange(timeStamp, mi, mx)) {
            console.log(req.pay_id, "time failed", timeStamp, tnano, mi, mx);
            console.log(
              req.pay_id,
              "time failed",
              sender,
              iso(timeStamp),
              iso(tnano),
              "\n",
              parseInt((nano(timeStamp) - nano(tnano)) / utils.mt),
              `https://polygonscan.com/tx/${tx.hash}`
            );
            return false;
          }

          let req_amt = parseFloat(req.req_amt) * 1e18;
          let [mia, mxa] = [req_amt - mimi, req_amt + mimi];
          console.log(amt, req_amt, _.inRange(amt, mia, mxa));
          if (_.isNaN(amt) || _.isNaN(req_amt)) {
            console.log("null amt failed");
            return false;
          }
          if (amt == req_amt) {
            return true;
          } else if (_.inRange(amt, mia, mxa)) {
            return true;
          } else {
            console.log("4");
            console.log("tx:", req.pay_id, req_amt);
            return false;
          }
          return true;
        });

        if (!got_request) continue;
        // console.log("found", got_request);
        got_request = {
          pay_id: got_request.pay_id,
          meta: { tx },
          status_code: 1,
          status: "paid",
        };
        update_paid = [...update_paid, got_request];
      }
    }
    let paid_ids = _.map(update_paid, "pay_id");
    console.log("found paid:", update_paid.length);
    await push_bulk(coll, update_paid, "update_paid");

    let now = utils.nano();
    // console.log(now);
    let failed_ids = _.map(list, (req) => {
      if (paid_ids.includes(req.pay_id)) return false;
      let tnano = utils.nano(req.date);
      if (_.inRange(now, tnano, tnano + allowed_buffer)) return false;
      return req.pay_id;
    });
    failed_ids = _.compact(failed_ids);
    // console.log(failed_ids);

    let to_fail_list =
      (await get_payments_list({
        token,
        before: st,
        after: null,
        status_code: 0,
      })) ?? [];
    let to_fail_ids = _.map(to_fail_list, "pay_id");
    // console.log(to_fail_list);
    failed_ids = [...failed_ids, ...to_fail_ids];
    console.log("found failed:", failed_ids.length, "(buffer time ended)");

    if (!_.isEmpty(failed_ids)) {
      await zed_db.db
        .collection(coll)
        .updateMany(
          { pay_id: { $in: failed_ids } },
          { $set: { status_code: -1, status: "failed" } }
        );
    }
    console.log(token, "check completed", utils.iso());
  }
};

const push_bulk = async (coll, obar, name = "-") => {
  try {
    if (_.isEmpty(obar))
      return console.log(`bulk@${coll} --`, `[${name}]`, "EMPTY");
    let bulk = [];
    obar = _.compact(obar);
    for (let ob of obar) {
      if (_.isEmpty(ob)) continue;
      let { pay_id } = ob;
      bulk.push({
        updateOne: {
          filter: { pay_id },
          update: { $set: ob },
          upsert: true,
        },
      });
    }
    await zed_db.db.collection(coll).bulkWrite(bulk);
    let len = obar.length;
    console.log(`bulk@${coll} --`, `[${name}]`, len);
  } catch (err) {
    console.log("err mongo bulk", coll, coll, obar && obar[0]?.hid);
    console.log(err);
  }
};

const test = async () => {
  let sender = "0x4915ec5b5170aa2099c63afd5400790b70b44070";
  console.log("danshan:", danshan_eth_address);
  let txns = await tokens_ob.WETH.get_txs({
    address: danshan_eth_address,
  });
  txns = txns.result;
  let hashs = _.map(txns, (e) => e.hash);
  let dbtxs = await zed_db.db
    .collection("payments")
    .find({ "meta.tx.hash": { $in: hashs } })
    .toArray();
  dbtxs = _.keyBy(dbtxs, (e) => getv(e, "meta.tx.hash"));
  let fin = txns.map((tx) => {
    let hash = tx.hash;
    let sender = tx.from;
    let reciever = tx.to;
    let date_tx = iso(parseFloat(tx.timeStamp) * 1000);
    let doc = dbtxs[hash];
    let amt = parseFloat(tx.value) / 1e18;
    let in_db = !_.isEmpty(doc);
    let db_doc = {
      req_amt: "-",
      pay_id: "-",
      service: "-",
      date: "-",
      status: "-",
      stable_name: "-",
    };
    if (in_db) {
      db_doc.req_amt = doc.req_amt;
      db_doc.pay_id = doc.pay_id;
      db_doc.service = doc.service;
      db_doc.date = doc.date;
      db_doc.status = doc.status;
      db_doc.stable_name = getv(doc, "meta_req.stable_name");
    }
    return { hash, sender, reciever, date_tx, amt, in_db, ...db_doc };
  });
  console.table(fin);
  await sheet_ops.sheet_print_ob(fin, {
    range: "payments",
    spreadsheetId: "1MWnILjDr71rW-Gp8HrKP6YnS03mJARygLSuS7xxsHhM",
  });
};

const test_2 = async () => {
  let sender = "0x55b76d32503e7c604d7ef2bab655fcfb31c2cafd";
  console.log({ sender });
  let txs = await tokens_ob["MATIC"].get_txs({ address: sender });
  console.log("txs", txs.result.length);
};

const runner = async () => {
  if (running) {
    console.log("############# pays already running.........");
    return;
  }
  try {
    running = 1;
    let st = moment().subtract(allowed_buffer, "millisecond").toISOString();
    let ed = moment().toISOString();
    await verify_user_payments([st, ed]);
    running = 0;
  } catch (err) {
    console.log("pays ERR\n", err);
    running = 0;
  }
};

const run_dur = async (st, ed) => {
  let offset = 60 * utils.mt;
  let now = nano(st);
  let eed = nano(ed);
  while (now < eed) {
    console.log("\n### payments_running");
    let a = iso(now);
    let b = iso(Math.min(now + offset, eed));
    console.log(a, "----->", b);
    await verify_user_payments([a, b], [0, -1]);
    now += offset;
  }
};

const run_cron = async () => {
  let cron_str = "*/30 * * * * *";
  cyclic_depedency.print_cron_details(cron_str);
  cron.schedule(cron_str, runner, { scheduled: true });
};

const fix = async () => {
  let st = moment()
    .subtract(3 * 60 * utils.mt, "millisecond")
    .toISOString();
  let ed = moment().toISOString();
  let list = await get_payments_list({
    token: "WETH",
    before: ed,
    after: st,
    status_code: -1,
  });
  let paid_list = _.map(list, (e) => {
    if (getv(e, "meta.tx.hash") != undefined) return e.pay_id;
    return false;
  });
  paid_list = _.compact(paid_list);
  console.log("list.len", list.length);
  console.log("paid_list.len", paid_list.length);
  if (!_.isEmpty(paid_list))
    await zed_db.db
      .collection("payments")
      .updateMany(
        { pay_id: { $in: paid_list } },
        { $set: { status: "paid", status_code: 1 } }
      );
};

const payments = {
  run_dur,
  runner,
  run_cron,
  test,
  test_2,
  fix,
};
module.exports = payments;
