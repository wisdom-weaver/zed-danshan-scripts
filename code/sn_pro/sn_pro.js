const _ = require("lodash");
const moment = require("moment");
const { zed_db } = require("../connection/mongo_connect");
const { print_cron_details } = require("../utils/cyclic_dependency");
const { cron_conf, getv, cdelay, iso } = require("../utils/utils");
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
    await cdelay(1000);
    await update_stable_state(pdoc.sender);
  } catch (err) {
    console.log("error at update_sdoc_after_paid", err.message);
  }
};

const get_sdoc = (stable) =>
  zed_db.db.collection("stables").findOne(fqstable(stable));

const gen_profile_status = (s) => {
  let profile_status = null;
  if (s.pro_registered == true) {
    if (s.sn_pro_active) {
      if (s.expires_at < iso()) profile_status = "expired";
      else profile_status = "active";
    } else {
      if (s.last_renew == null) profile_status = "new";
      else if (s.expires_at > iso()) profile_status = "blocked";
      else if (s.expires_at < iso()) profile_status = "expired";
    }
  } else {
    profile_status = "not_registered";
  }
  return profile_status;
};

const update_stable_state = async (stable) => {
  let s = await get_sdoc(stable);
  if (!s) throw new Error("stable not found");
  let upd = { stable: s.stable };

  upd.profile_status = gen_profile_status(s);

  if (upd.profile_status !== "active") upd.sn_pro_active = false;
  else upd.sn_pro_active = true;

  if (!_.isEmpty(upd)) {
    await zed_db.db
      .collection("stables")
      .updateOne(fqstable(stable), { $set: upd });
  }
  console.log(stable, upd);
};

const update_all_stables = async () => {
  let stables = await zed_db.db
    .collection("stables")
    .find({ pro_registered: true }, { projection: { _id: 0, stable: 1 } })
    .toArray();
  stables = _.map(stables, "stable");
  console.log("stables.len", stables.length);

  for (let chu of _.chunk(stables, 3))
    await Promise.all(chu.map((e) => update_stable_state(e)));
  console.log("ended");
};

const txnchecker = async () => {
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

const run_cron_txns = async () => {
  // txns
  const cron_str0 = "*/15 * * * * *";
  print_cron_details(cron_str0);
  cron.schedule(cron_str0, txnchecker, cron_conf);
};

const run_cron_stables = async () => {
  // stables
  const cron_str = "0 * * * * *";
  print_cron_details(cron_str);
  cron.schedule(cron_str, update_all_stables, cron_conf);
};

const test = async () => {
  console.log("test");
  const stable = "0xc2014B17e2234ea18a50F292faEE29371126A3e0";
  await update_stable_state(stable);
};

const main_runner = async () => {
  console.log("## sn_pro");
  let [n, f, a1, a2, a3, a4, a5] = process.argv;
  if (a2 == "test") await test();
  if (a2 == "runner") await txnchecker();
  if (a2 == "run_cron_txns") await run_cron_txns();
  if (a2 == "run_cron_stables") await run_cron_stables();
};

const sn_pro = {
  main_runner,
};
module.exports = sn_pro;
