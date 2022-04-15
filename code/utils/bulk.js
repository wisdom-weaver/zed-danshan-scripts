const { zed_db } = require("../connection/mongo_connect");
const { get_ed_horse } = require("./cyclic_dependency");
const _ = require("lodash");
const { get_hids } = require("./utils");

const try_fn = async (fn, name, ...args) => {
  try {
    return await fn(args);
  } catch (err) {
    console.log("err at", name);
    console.log(err.name, ":", err.message);
    return null;
  }
};

const run_bulk_only = async (
  name = "fn",
  fn = () => {},
  coll = "test_mode_coll",
  hids = [],
  cs = 300,
  test_mode = 0
) => {
  if (_.isEmpty(hids)) return console.log("empty run_bulk", name);
  for (let chunk of _.chunk(hids, cs)) {
    let [a, b] = [chunk[0], chunk[chunk.length - 1]];
    console.log(name, ":", a, "->", b);
    let obar = await Promise.all(chunk.map((hid) => try_fn(fn, name, hid)));
    obar = _.compact(obar);
    if (obar.length !== chunk.length) {
      let err_n = Math.abs(obar.length - chunk.length);
      console.log("failed", err_n);
    }
    if (!test_mode) await push_bulk(coll, obar, name);
  }
  console.log("ended", name);
};

const run_bulk_range = async (
  name = "fn",
  fn = () => {},
  coll = "test_mode_coll",
  st = 1,
  ed = 1,
  cs = 300,
  test_mode = 0
) => {
  if (ed == "ed") ed = await get_ed_horse();
  let hids = get_hids(st, ed);
  await run_bulk_only(name, fn, coll, hids, cs);
  console.log("ended", name);
};

const run_bulk_all = async (
  name = "fn",
  fn = () => {},
  coll = "test_mode_coll",
  cs = 300,
  test_mode = 0
) => {
  let [st, ed] = [1, await get_ed_horse()];
  let hids = get_hids(st, ed);
  await run_bulk_only(name, fn, coll, hids, cs);
  console.log("ended", name);
};

const push_bulk = async (coll, obar, name = "-") => {
  try {
    if (_.isEmpty(obar))
      return console.log(`bulk@${coll} --`, `[${name}]`, "EMPTY");
    let bulk = [];
    obar = _.compact(obar);
    for (let ob of obar) {
      if (_.isEmpty(ob)) continue;
      let { hid } = ob;
      bulk.push({
        updateOne: {
          filter: { hid },
          update: { $set: ob },
          upsert: true,
        },
      });
    }
    await zed_db.db.collection(coll).bulkWrite(bulk);
    let len = obar.length;
    let sth = obar[0].hid;
    let edh = obar[obar.length - 1].hid;
    console.log(`bulk@${coll} --`, `[${name}]`, len, "..", sth, "->", edh);
  } catch (err) {
    console.log("err mongo bulk", coll, coll, obar && obar[0]?.hid);
    console.log(err);
  }
};

const push_bulkc = async (coll, obar, name = "-", key) => {
  try {
    if (_.isEmpty(obar))
      return console.log(`bulk@${coll} --`, `[${name}]`, "EMPTY");
    let bulk = [];
    obar = _.compact(obar);
    for (let ob of obar) {
      if (_.isEmpty(ob)) continue;
      if (!ob[key]) continue;
      bulk.push({
        updateOne: {
          filter: { [key]: ob[key] },
          update: { $set: ob },
          upsert: true,
        },
      });
    }
    // JSON.stringify(bulk);
    await zed_db.db.collection(coll).bulkWrite(bulk);
    let len = obar.length;
    let sth = obar[0][key];
    let edh = obar[obar.length - 1][key];
    console.log(`bulk@${coll} --`, `[${name}]`, len, "..", sth, "->", edh);
  } catch (err) {
    console.log("err mongo bulk", coll, coll, obar && obar[0][key]);
    console.log(err);
  }
};

const bulk = {
  run_bulk_all,
  run_bulk_range,
  run_bulk_only,
  push_bulk,
  push_bulkc,
};
module.exports = bulk;
