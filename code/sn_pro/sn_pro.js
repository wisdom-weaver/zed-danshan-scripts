const _ = require("lodash");
const moment = require("moment");
const { zed_db } = require("../connection/mongo_connect");
const { print_cron_details } = require("../utils/cyclic_dependency");
const { cron_conf, getv, cdelay, iso } = require("../utils/utils");
const cron = require("node-cron");
const jwt = require("jsonwebtoken");

const coll = "stables";
const eval_failed = true;
const pay_toll = [15, "minutes"];
const fqstable = (stable) => ({ stable: { $regex: stable, $options: "i" } });

const get_bod = (req) => ({ ...req.query, ...req.body, ...req.params });

// const subs_dur = [30, "minutes"];
const subs_dur = [30, "days"];
const mil = {
  month: 30 * 24 * 60 * 60 * 1000,
  days: 24 * 60 * 60 * 1000,
  minutes: 60 * 1000,
  seconds: 1000,
};
const update_sdoc_after_paid = async (stable) => {
  try {
    console.log("update_stable");
    let sdoc = await zed_db.db.collection("stables").findOne(fqstable(stable), {
      projection: { _id: 0, stable: 1, sn_pro_txns: 1, last_renew: 1 },
    });
    if (_.isEmpty(sdoc)) throw new Error("no such stable");
    let fmaxdate = null; //moment().subtract(30, "minutes").toISOString();
    let payids = sdoc.sn_pro_txns ?? [];
    if (_.isEmpty(payids)) return;
    let paids = await zed_db.db
      .collection("payments")
      .find(
        {
          status_code: 1,
          pay_id: { $in: payids },
          ...(!_.isNil(sdoc?.last_renew)
            ? { date: { $gt: sdoc.last_renew } }
            : {}),
        },
        { projection: { _id: 0 } }
      )
      .limit(1)
      .toArray();

    // console.table(paids);

    if (_.isEmpty(paids)) {
      console.log("emp paids");
      return;
    }
    let pdoc = getv(paids, 0);
    let { date, pay_id } = pdoc;
    if (pdoc.date == sdoc.last_renew) {
      console.log("done already");
      return;
    }

    let last_renew = date;
    let expires_at = moment(last_renew)
      .add(...subs_dur)
      .toISOString();

    const stable0 = stable.toLowerCase();
    const salt = process.env.TOKEN_KEY + stable0;
    const token = jwt.sign(
      {
        stable: stable.toLowerCase(),
        expires_at,
      },
      salt,
      { expiresIn: 1e14 }
    );

    await zed_db.db.collection("stables").updateOne(fqstable(stable), {
      $set: {
        sn_pro_active: true,
        expires_at,
        last_renew,
        token,
      },
    });
    await cdelay(1000);
    await update_stable_state(pdoc.sender);

    await zed_db.db.collection("payments").updateOne(
      { pay_id },
      {
        $set: {
          "meta_req.sn_pro_marked": true,
          "meta_req.sn_pro_start": date,
          "meta_req.sn_pro_end": expires_at,
        },
      }
    );
  } catch (err) {
    console.log("error at update_sdoc_after_paid", err.message);
    console.log(err);
  }
};

const get_sdoc = (stable) =>
  zed_db.db.collection("stables").findOne(fqstable(stable));

const gen_profile_status = (s) => {
  s.profile_status = null;
  if (s.pro_registered == true) {
    if (s.sn_pro_blocked) {
      s.profile_status = "blocked";
    } else {
      if (s.last_renew == null) s.profile_status = "new";
      else if (s.expires_at > iso()) s.profile_status = "active";
      else if (s.expires_at < iso()) s.profile_status = "expired";
    }
  } else {
    s.profile_status = "not_registered";
  }
  if (s.profile_status == "active") s.sn_pro_active = true;
  else s.sn_pro_active = false;
  return s.profile_status;
};

const update_stable_state = async (stable) => {
  let s = await get_sdoc(stable);
  if (!s) throw new Error("stable not found");
  let upd = { stable: s.stable };

  upd.profile_status = gen_profile_status(s);
  let sn_pro_active = null;
  if (upd.profile_status == "active") sn_pro_active = true;
  else sn_pro_active = false;
  upd.sn_pro_active = sn_pro_active;

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
        "meta_req.sn_pro_marked": { $ne: true },
      },
      { projection: { _id: 0, sender: 1, pay_id: 1 } }
    )
    .toArray();

  if (_.isEmpty(paids)) {
    console.log("paids: empty nothing paid");
  } else {
    console.table(paids);
    console.log("paids", paids.length);
    for (let { pay_id, sender } of paids) {
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

const check_new = async () => {
  console.log("check_new");
  let ss = await zed_db.db
    .collection("stables")
    .find({ profile_status: "new" }, { projection: { stable: 1 } })
    .toArray();
  ss = _.map(ss, "stable");
  for (let stable of ss) await update_sdoc_after_paid(stable);
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

  const cron_str1 = "0 */10 * * * *";
  print_cron_details(cron_str1);
  cron.schedule(cron_str, check_new, cron_conf);
};

const test = async () => {};

const main_runner = async () => {
  console.log("## sn_pro");
  let [n, f, a1, a2, a3, a4, a5] = process.argv;
  console.log(process.argv);

  if (a2 == "test") await test();
  if (a2 == "runner") await txnchecker();
  if (a2 == "run_cron_txns") await run_cron_txns();
  if (a2 == "run_cron_stables") await run_cron_stables();
  if (a2 == "update_stable") {
    await update_sdoc_after_paid(a3);
    await update_stable_state(a3);
  }
  if (a2 == "check_new") await check_new();
};

const sn_pro = {
  main_runner,
};
module.exports = sn_pro;
