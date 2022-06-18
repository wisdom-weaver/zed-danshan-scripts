const _ = require("lodash");
const { zed_db, zed_ch } = require("../connection/mongo_connect");
const bulk = require("../utils/bulk");
const cyclic_depedency = require("../utils/cyclic_dependency");
const {
  get_races_n,
  get_races_of_hid,
  get_range_hids,
} = require("../utils/cyclic_dependency");
const r4data = require("./r4");
const cc = require("./color_codes");
const ancestors = require("./ancestors");
const sheet_ops = require("../../sheet_ops/sheets_ops");
const { promises_n, iso, pad } = require("../utils/utils");
const utils = require("../utils/utils");
const { get_ancesters_stub, get_ancesters } = require("./ancestors");
const cron = require("node-cron");

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

const only = async (hids) => {
  hids = await cyclic_depedency.filt_valid_hids(hids);
  bulk.run_bulk_only(name, generate, coll, hids, cs, test_mode);
};

const range = async (st, ed) => {
  st = parseInt(st);
  ed = parseInt(ed);
  if (!ed || ed == "ed") ed = await cyclic_depedency.get_ed_horse();
  console.log({ st, ed });
  for (let i = st; i <= ed; i += cs) {
    /*
┌─────────┬───────────┬────────┬────────┐
│ (index) │    _id    │ minhid │ maxhid │
├─────────┼───────────┼────────┼────────┤
│    0    │ '010 1.1' │  4545  │ 19661  │
│    1    │ '020 050' │  4510  │ 19674  │
│    2    │ '050 100' │  4447  │ 19670  │
│    3    │ '100 150' │  4452  │ 19655  │
│    4    │ '100 200' │  4381  │ 47640  │
│    5    │ '150 250' │  4553  │ 19386  │
│    6    │ '250 500' │  4464  │ 42436  │
│    7    │ '500 100' │  4363  │ 57422  │
│    8    │   'def'   │  4356  │ 429966 │
└─────────┴───────────┴────────┴────────┘
    */

    cs = 1;
    let [a, b] = [i, i + cs - 1];
    let hids = await cyclic_depedency.filt_valid_hids_range(a, b);
    console.log(`hids: ${a} -> ${b} :`, hids.length);
    await bulk.run_bulk_only(name, generate, coll, hids, cs, test_mode);
  }
};

const all = async () => {
  return range(1, null);
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
  let ar = await zed_db.db
    .collection("horse_details")
    .aggregate([
      {
        $project: {
          hid: 1,
          mid: "$parents.mother",
          fid: "$parents.father",
        },
      },
      {
        $match: {
          mid: {
            $ne: null,
          },
          fid: {
            $ne: null,
          },
        },
      },
      {
        $project: {
          hid: 1,
          mdiff: {
            $abs: {
              $subtract: ["$hid", "$mid"],
            },
          },
          fdiff: {
            $abs: {
              $subtract: ["$hid", "$fid"],
            },
          },
        },
      },
      {
        $project: {
          hid: 1,
          diff: {
            $min: ["$fdiff", "$mdiff"],
          },
        },
      },
      {
        $sort: {
          diff: 1,
        },
      },
      {
        $group: {
          _id: {
            $switch: {
              branches: [
                // [0, 0],
                [1, 1.1],
                [2, 5],
                [5, 10],
                [10, 15],
                [15, 25],
                [25, 50],
                [50, 100],
                [100, 200],
              ].map(([a, b]) => {
                return {
                  case: {
                    $and: [{ $lt: ["$diff", b] }, { $gte: ["$diff", a] }],

                    // $and:[
                    //   // ['$gte', ["$diff", a]],
                    //   ['$lt', ["$diff", b]],
                    // ]
                  },
                  then: `${_.pad(a, 3, "0")} ${_.pad(b, 3, "0")}`,
                };
              }),
              default: "def",
            },
          },
          minhid: {
            $min: "$hid",
          },
          maxhid: {
            $max: "$hid",
          },
        },
      },
      {
        $sort: {
          _id: 1,
        },
      },
    ])
    .toArray();
  console.table(ar);
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

const run_cron = async () => {
  const cron_str = `0 0 * * 1,4`;
  cyclic_depedency.print_cron_details(cron_str);
  cron.schedule(cron_str, async () => {
    console.log("started", iso());
    await all();
    console.log("ended", iso());
  });
};

const line = {
  calc,
  generate,
  all,
  only,
  range,
  test,
  fix,
  pair_test,
  run_cron,
};
module.exports = line;
