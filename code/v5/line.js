const _ = require("lodash");
const { zed_db, zed_ch } = require("../connection/mongo_connect");
const bulk = require("../utils/bulk");
const cyclic_depedency = require("../utils/cyclic_dependency");
const { get_races_n, get_races_of_hid } = require("../utils/cyclic_dependency");
const r4data = require("./r4");
const cc = require("./color_codes");
const ancestors = require("./ancestors");
const sheet_ops = require("../../sheet_ops/sheets_ops");
const { promises_n } = require("../utils/utils");
const utils = require("../utils/utils");
const { get_ancesters_stub, get_ancesters } = require("./ancestors");

const name = "line";
const coll = "line";
let cs = 1;
let test_mode = 0;

const get_codes_val = async (hid) => {
  try {
    let races_n = (await get_races_n(hid)) || 0;
    let { rng, dp, ba } = await r4data.get_r4(hid);
    if (test_mode) console.log(hid, { races_n, rng, dp, ba });
    let [r = 0, d = 0, b = 0] = [
      cc.get_rng_val(rng),
      cc.get_dp_val(dp),
      cc.get_ba_val(ba),
    ];
    let count = _.filter(
      [r, d, b],
      (e) => ![null, NaN, undefined, 0].includes(e)
    )?.length;
    return { races_n, r, d, b, tot: r + d + b, count };
  } catch (err) {
    console.log(err);
    console.log(err.message);
    return { r: 0, d: 0, b: 0, tot: 0, count: 0 };
  }
};

const calc = async ({ hid }) => {
  const tree = await ancestors.get_ancesters({ hid });
  if (test_mode) console.table(tree);
  if (_.isEmpty(tree)) {
    return { hid, tree: [], adj_total: 0, count_total: 0, line_score: null };
  }
  let hids_ccs = await promises_n(
    _.map(tree, "0").map((hid) => get_codes_val(hid).then((d) => [hid, d])),
    10
  );
  hids_ccs = hids_ccs.filter(([hid, d]) => {
    if (d.tot == undefined) console.log("undef", hid, d);
    return d?.races_n != 0;
  });
  if (_.isEmpty(hids_ccs)) {
    return { hid, tree, adj_total: 0, count_total: 0, line_score: null };
  }
  hids_ccs = _.fromPairs(hids_ccs);

  let ob = tree.map(([hid, k, level]) => {
    let eaob = hids_ccs[hid];
    if (eaob == undefined) return undefined;
    let wt = level_wt(level);
    let adjusted = wt * eaob.tot;
    return { hid, k, level, wt, ...eaob, adjusted };
  });
  ob = _.compact(ob);

  if (test_mode) console.table(ob);
  const adj_total = _.sumBy(ob, "adjusted") ?? 0;
  const count_total = _.sumBy(ob, "count") ?? 0;
  const line_score = (adj_total || 0) / (count_total || 1);
  let ret = { hid, tree, adj_total, count_total, line_score };
  if (test_mode) console.log(ret);
  return ret;
};

const generate = async (hid) => {
  try {
    hid = parseInt(hid);
    let ob = await calc({ hid });
    // if (test_mode) console.log(ob);
    return ob;
  } catch (err) {
    console.log(err);
  }
};

const all = async () => bulk.run_bulk_all(name, generate, coll, cs, test_mode);
const only = async (hids) =>
  bulk.run_bulk_only(name, generate, coll, hids, cs, test_mode);
const range = async (st, ed) => {
  st = parseInt(st);
  ed = parseInt(ed);
  if (!ed || ed == "ed") ed = await cyclic_depedency.get_ed_horse();
  bulk.run_bulk_range(name, generate, coll, st, ed, cs, test_mode);
};

const fix = async () => {
  let stable = "0xa0d9665E163f498082Cd73048DA17e7d69Fd9224";
  let hids = await cyclic_depedency.get_owner_horses_zed_hids({ oid: stable });
  for (let hid of hids) {
    await only([hid]);
    console.log("done", hid);
  }
};

const level_wt = (level) => {
  if (level < 1) return 0;
  if (level == 1) return 1;
  if (level == 2) return 0.95;
  if (level == 3) return 0.92;
  if (level == 4) return 0.89;
  return 0.8;
};

const test = async (hids) => {
  // test_mode = 1;
  // for (let hid of hids) {
  //   let ob = await calc({ hid });
  //   console.log(ob);
  // }
  // console.log("done");
  let [hid,row] = hids;
  let baby_rng = await r4data.get_rng(hid);
  let ans = await get_ancesters({ hid });
  let ans_rngs = await Promise.all(
    _.map(ans, 0).map((h) => r4data.get_rng(h).then((d) => [h, d]))
  );
  ans_rngs = _.fromPairs(ans_rngs);
  let ob = ans.map(([hid, k, level]) => {
    let rng = ans_rngs[hid];
    return { hid, k, level, rng };
  });
  ob = [{ hid, k: "baby", level: 0, rng: baby_rng }, ...ob];
  console.table(ob);
  await sheet_ops.sheet_print_ob(ob, {
    spreadsheetId: "1MWnILjDr71rW-Gp8HrKP6YnS03mJARygLSuS7xxsHhM",
    range: `baby_rng_ances!A${row}`,
  });
};

const pair_test = async (ar) => {
  let [father_id, mother_id] = ar;
  const ob = await get_ancesters_stub({
    mother_id,
    father_id,
    level: 2,
    k: "",
    scratch: 1,
  });
  console.table(ob);
};

const line = { calc, generate, all, only, range, test, fix, pair_test };
module.exports = line;
