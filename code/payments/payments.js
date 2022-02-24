const _ = require("lodash");
const { zed_db } = require("../connection/mongo_connect");
const cyclic_depedency = require("../utils/cyclic_dependency");
const utils = require("../utils/utils");
const etherscan = require("./etherscan");
const polygonscan = require("./polygonscan");
const moment = require("moment");
const cron = require("node-cron");

const allowed_buffer = 15 * utils.mt;
const mimi = 10;
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

const get_payments_list = async ({ before, after, token }) => {
  let query = {};
  if (token) query.token = token;
  if (after || before) query.date = {};
  if (after) query.date.$gte = utils.iso(after);
  if (before) query.date.$lte = utils.iso(before);
  query.status_code = { $in: [0] };
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
const verify_user_payments = async ([st, ed]) => {
  console.log("verify_user_payments\n", st, "->", ed);
  for (let { token, get_balance, get_txs } of _.values(tokens_ob)) {
    console.log("\n------------\n");
    console.log("started", utils.iso());
    console.log("token:", token);
    let list = await get_payments_list({ token, before: ed, after: st });

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
    console.log("all_txns:", all_txns.length);

    let existing_txns = await get_if_existing_txns(_.map(all_txns, "hash"));
    console.log("existing_txns:", existing_txns.length);

    for (let tx of all_txns) {
      // console.log("tx:", tx.hash);
      if (existing_txns.includes(tx.hash)) {
        // console.log("tx exists already");
        continue;
      }
      let amt = parseFloat(tx.value);
      let timeStamp = parseFloat(tx.timeStamp) * 1000;
      for (let [sender, sender_reqs] of _.entries(list_gp)) {
        if (tx.from !== sender) continue;
        let got_request = _.find(sender_reqs, (req) => {
          if (!(req.sender == tx.from)) return false;
          if (!(req.reciever == tx.to)) return false;
          let tnano = utils.nano(req.date);
          console.log(req.date, tnano);
          if (!_.inRange(timeStamp, tnano, tnano + allowed_buffer))
            return false;
          let req_amt = req.req_amt * 1e18;
          if (!_.inRange(amt, req_amt - mimi, req_amt + mimi)) return false;
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
    console.log("found failed:", failed_ids.length, "(buffer time ended)");
    console.log(failed_ids);

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
  let test_doc = {
    pay_id: "b0c8f5",
    sender: "0x4e8c5dcd73df0448058e28b5205d1c63df7b30d9",
    reciever: "0xa0d9665e163f498082cd73048da17e7d69fd9224",
    req_amt: 0.01,
    token: "MATIC",
    service: "get-payment-test",
    status: "pending",
    status_code: 0,
    date: "2021-04-01T17:48:51.000Z",
    meta_req: {},
    meta: {},
  };
  pay_id = test_doc.pay_id;
  await zed_db.db.collection(coll).deleteOne({ pay_id });
  console.log("deleted doc", pay_id);
  await zed_db.db.collection(coll).insertOne(test_doc);
  console.log("inserted doc", pay_id);
  console.log("waiting 1 minute");
  await utils.delay(60 * 1000);
  let st = "2021-04-01T17:48:50.000Z";
  let ed = "2021-04-01T17:48:53.000Z";
  await verify_user_payments([st, ed]);
};

const runner = async () => {
  let st = moment().subtract(allowed_buffer, "millisecond").toISOString();
  let ed = moment().toISOString();
  await verify_user_payments([st, ed]);
};

const run_dur = async ([st, ed]) => {
  st = utils.iso(st);
  ed = utils.iso(ed);
  await verify_user_payments([st, ed]);
};

const run_cron = async () => {
  let cron_str = "0 * * * * *";
  cyclic_depedency.print_cron_details(cron_str);
  cron.schedule(cron_str, runner, { scheduled: true });
};

const payments = {
  run_dur,
  runner,
  run_cron,
  test,
};
module.exports = payments;
