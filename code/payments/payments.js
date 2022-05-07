const _ = require("lodash");
const { zed_db } = require("../connection/mongo_connect");
const cyclic_depedency = require("../utils/cyclic_dependency");
const utils = require("../utils/utils");
const etherscan = require("./etherscan");
const polygonscan = require("./polygonscan");
const moment = require("moment");
const cron = require("node-cron");
const crypto = require("crypto");
const { iso, getv } = require("../utils/utils");
const sheet_ops = require("../../sheet_ops/sheets_ops");

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
  // console.log(query);
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

const verify_user_payments = async (st, ed) => {
  console.log("verify_user_payments\n", st, "->", ed);
  for (let { token, get_balance, get_txs } of _.values(tokens_ob)) {
    console.log("\n------------\n");
    console.log("started", utils.iso());
    console.log("token:", token);
    let list = await get_payments_list({
      token,
      before: ed,
      after: st,
      status_code: 0,
    });

    let update_paid = [];

    console.log("pending:", list.length);
    if (list.length == 0) continue;
    // list = _.keyBy(list, "pay_id");
    let recievers = _.uniq(_.map(list, "reciever"));
    let list_gp = _.groupBy(list, "sender");
    console.log("recievers", recievers);
    let all_txns = [];
    for (let reciever of recievers) {
      let txns = await get_txs({ address: reciever });
      // console.log(txns)
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

    all_txns = [...all_txns, ...dummies];
    console.log("all_txns[updated]:", all_txns.length);

    let existing_txns = await get_if_existing_txns(_.map(all_txns, "hash"));
    console.log("existing_txns:", existing_txns.length);

    for (let tx of all_txns) {
      if (existing_txns.includes(tx.hash)) {
        console.log("tx exists already");
        continue;
      }
      let amt = parseFloat(tx.value);
      let timeStamp = parseFloat(tx.timeStamp) * 1000;
      for (let [sender, sender_reqs] of _.entries(list_gp)) {
        if (tx.from.toLowerCase() !== sender.toLowerCase()) continue;
        let got_request = _.find(sender_reqs, (req) => {
          if (!(req?.sender?.toLowerCase() == tx.from.toLowerCase()))
            return false;
          // console.log("1");

          if (!(req?.reciever?.toLowerCase() == tx.to.toLowerCase()))
            return false;
          // console.log("2");

          let tnano = utils.nano(req.date);
          let [mi, mx] = [tnano - allowed_buffer, tnano + allowed_buffer];
          if (!_.inRange(timeStamp, mi, mx)) {
            console.log("time failed");
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

const runner = async () => {
  let st = moment().subtract(allowed_buffer, "millisecond").toISOString();
  let ed = moment().toISOString();
  await verify_user_payments(st, ed);
};

const run_dur = async (st, ed) => {
  st = utils.iso(st);
  ed = utils.iso(ed);
  await verify_user_payments(st, ed);
};

const run_cron = async () => {
  let cron_str = "0 * * * * *";
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
  fix,
};
module.exports = payments;
