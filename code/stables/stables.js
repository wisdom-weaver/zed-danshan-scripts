const moment = require("moment");
const _ = require("lodash");
const { zed_ch, zed_db } = require("../connection/mongo_connect");
const {
  get_owner_horses_zed,
  get_ed_horse,
  jparse,
} = require("../utils/cyclic_dependency");
const { getv, iso } = require("../utils/utils");

const coll = "stables";

const run_stable = async (stable) => {
  console.log("run_stable", stable);
  let ar = await get_owner_horses_zed({ oid: stable });
  if (_.isEmpty(ar)) return console.log("stable has no horses");
  let doc = getv(ar, "0");
  const {
    owner: stable0,
    owner_stable: stable_name,
    owner_stable_slug: stable_slug,
  } = doc;
  let stable_doc = {
    stable: stable0,
    stable0: stable0.toLowerCase(),
    stable_name,
    stable_slug,
  };
  console.log(stable_doc);
  let horses = ar.map((e) => {
    let name = getv(e, "hash_info.name");
    let hid = getv(e, "horse_id");
    return { hid, name };
  });
  let hids = _.map(horses, "hid");
  let horses_n = horses.length;
  stable_doc = {
    ...stable_doc,
    horses_n,
    horses,
    hids,
  };
  await zed_db.db
    .collection(coll)
    .updateOne(
      { stable: { $regex: `${stable.toLowerCase()}`, $options: "i" } },
      { $set: stable_doc },
      { upsert: true }
    );
  if (!_.isEmpty(hids))
    await zed_db.db
      .collection("horse_details")
      .updateMany(
        { hid: { $in: hids } },
        { $set: { oid: stable0, transfer_date: iso() } }
      );
  console.log("done", horses_n, "horses");
  // return stable_doc;
};

const run_stables = async (stables) => {
  i = 0;
  for (let chu of _.chunk(stables, 5)) {
    try {
      await Promise.all(chu.map((stable) => run_stable(stable)));
    } catch (err) {
      console.log(err.message);
    }
    i += chu.length;
    console.log("done", i);
  }
};

const update_all_stables = async ([st, ed]) => {
  console.log("update_all_stables");
  let stabs = [];
  console.log([st, ed]);
  let scs = 1000;
  for (let i = st; i <= ed; ) {
    let docs = await zed_db.db
      .collection("horse_details")
      .find(
        { hid: { $gte: i, $lt: i + scs } },
        { projection: { hid: 1, oid: 1 } }
      )
      .toArray();
    let oids = _.map(docs, "oid") ?? [];
    console.log({ $gte: i, $lt: i + scs }, oids.length);
    stabs.push(oids);
    i += scs;
  }
  stabs = _.chain(stabs).compact().flatten().compact().uniq().value();
  stabs = _.reverse(stabs);
  console.log("stables.len", stabs.length);

  i = 0;
  for (let chu of _.chunk(stabs, 5)) {
    try {
      await Promise.all(chu.map((stable) => run_stable(stable)));
    } catch (err) {
      console.log(err.message);
    }
    i += chu.length;
    console.log("done", i);
  }
  console.log("done all");
};

const get_missing_stables_in = async (stables) => {
  stables = _.chain(stables)
    .compact()
    .filter((e) => e != "0x0000000000000000000000000000000000000000")
    .map((e) => e.toLowerCase())
    .uniq()
    .value();
  let exists = await zed_db.db
    .collection("stables")
    .find({ stable0: { $in: stables } }, { projection: { stable0: 1 } })
    .toArray();
  exists = _.map(exists, "stable0");
  let diff = _.difference(stables, exists);
  return diff;
};

const fix_stable_horses = async () => {
  let old_date = moment().add(-10, "days").toISOString();
  let stables = await zed_db.db
    .collection("stables")
    .find({}, { projection: { stable0: 1, horses: 1 } })
    // .limit(3)
    .toArray();
  console.log("all stables", stables.length);
  let i = 0;
  for (let stable of stables) {
    ++i;
    let { stable0, horses } = stable;
    let hids = _.map(horses, "hid");
    if (!_.isEmpty(hids))
      await zed_db.db
        .collection("horse_details")
        .updateMany(
          { hid: { $in: hids } },
          { $set: { oid: stable0, transfer_date: old_date } }
        );
    console.log(i, stable0, "n:", hids.length);
  }
};

const fix_sn_pro_horses_class = async () => {
  let users = await zed_db.db
    .collection("stables")
    .find({ pro_registered: true }, { projection: { stable0: 1 } })
    .limit(3)
    .toArray();
  for (let u of users) {
    let stable = u.stable0;
    console.log(u.stable0);
    let ar = await get_owner_horses_zed({ oid: stable });
    ar = _.map(ar, (e) => ({ hid: e.horse_id, tc: e.class }));
    console.table(ar);
  }
};

const main_runner = async () => {
  let [_node, _cfile, arg1, arg2, arg3, arg4, arg5] = process.argv;
  console.log("stables");
  if (arg2 == "run") await run_stable(arg3);
  if (arg2 == "all") {
    arg3 = jparse(arg3);
    let st = getv(arg3, "0") ?? 1;
    let ed = getv(arg3, "1") ?? 550000;
    await update_all_stables([st, ed]);
  }
  if (arg2 == "test") await test();
  if (arg2 == "fix_stable_horses") await fix_stable_horses();
  if (arg2 == "fix_sn_pro_horses_class") await fix_sn_pro_horses_class();
};

const stables_s = {
  main_runner,
  get_missing_stables_in,
  run_stable,
  run_stables,
};
module.exports = stables_s;
