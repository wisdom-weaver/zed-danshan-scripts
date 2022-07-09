const _ = require("lodash");
const moment = require("moment");
const { zed_db } = require("../connection/mongo_connect");
const { print_cron_details } = require("../utils/cyclic_dependency");
const { cron_conf, getv } = require("../utils/utils");
const cron = require("node-cron");

const coll = "stables";
const eval_failed = false;
const pay_toll = [15, "minutes"];
const fqstable = (stable) => ({ stable: { $regex: stable, $options: "i" } });

const get_bod = (req) => ({ ...req.query, ...req.body, ...req.params });

const update_sdoc_after_paid = async (stable) => {
  try {
    let sdoc = await zed_db.db.collection("stables").findOne(fqstable(stable), {
      projection: { _id: 0, stable: 1, sn_pro_txns: 1 },
    });
    if (_.isEmpty(sdoc)) throw new Error("no such stable");
    let fmaxdate = moment().subtract(30, "minutes").toISOString();
    let payids = sdoc.sn_pro_txns ?? [];
    if (_.isEmpty(payids)) return;
    let paids = await zed_db.db
      .collection("payments")
      .find(
        {
          status_code: 1,
          pay_id: { $in: payids },
          date: { $gte: fmaxdate },
        },
        { projection: { _id: 0 } }
      )
      .limit(1)
      .toArray();
    if (_.isEmpty(paids)) {
      console.log("notpaid");
      return;
    }
    let pdoc = getv(paids, 0);
    let { date } = pdoc;
    let last_renew = date;
    let expires_at = moment(last_renew).add(31, "days").toISOString();
    await zed_db.db.collection("stables").updateOne(fqstable(stable), {
      $set: {
        sn_pro_active: true,
        expires_at,
        last_renew,
      },
    });
  } catch (err) {
    console.log("error at update_sdoc_after_paid", err.message);
  }
};

const runner = async () => {
  if (!zed_db) return;
  let fmxdate = moment()
    .subtract(...pay_toll)
    .toISOString();
  let paids = await zed_db.db
    .collection("payments")
    .find(
      {
        service: "buy:sn_pro",
        status_code: 1,
        date: { $gte: fmxdate },
      },
      { projection: { _id: 0, sender: 1, pay_id: 1 } }
    )
    .toArray();
  if (_.isEmpty(paids)) {
    console.log("paids: empty nothing paid");
  } else {
    console.table(paids);
    console.log("paids", paids.length);
    for (let { sender } of paids) {
      await update_sdoc_after_paid(sender);
    }
  }

  if (eval_failed) {
    let failed = await zed_db.db
      .collection("payments")
      .find(
        {
          service: "buy:sn_pro",
          status_code: 0,
          date: { $lte: fmxdate },
        },
        { projection: { _id: 0, sender: 1, pay_id: 1 } }
      )
      .toArray();

    if (_.isEmpty(failed)) {
      console.log("failed: empty nothing found");
    } else {
      console.table(failed);
      let failedids = _.map(failed, "pay_id");
      await zed_db.db
        .collection("payments")
        .updateMany(
          { pay_id: { $in: failedids } },
          { $set: { status_code: -1, status: "failed" } }
        );
    }
  }
};

const run_cron = async () => {
  let cron_str = "*/15 * * * * *";
  print_cron_details(cron_str);
  cron.schedule(cron_str, runner, cron_conf);
};

const test = async () => {
  console.log("test");
};

const main_runner = async () => {
  console.log("## sn_pro");
  let [n, f, a1, a2, a3, a4, a5] = process.argv;
  if (a2 == "test") await test();
  if (a2 == "runner") await runner();
  if (a2 == "run_cron") await run_cron();
};

const sn_pro = {
  main_runner,
};
module.exports = sn_pro;
