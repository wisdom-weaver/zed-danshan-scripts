const { get_races_of_hid } = require("../utils/cyclic_dependency");
const mdb = require("../connection/mongo_connect");
const _ = require("lodash");
const bulk = require("../utils/bulk");
const { zed_db } = require("../connection/mongo_connect");
const utils = require("../utils/utils");
const cyclic_depedency = require("../utils/cyclic_dependency");
const coll = "rating_flames3";
const name = "rating_flames v3";
let test_mode = 0;

let f_ob = {
  undefined: 0,
  A: 6,
  B: 5,
  C: 4,
  D: 3,
  E: 2,
  F: 1,
};

const get_docs = async (st, ed) => {
  let docs = await zed_db.db
    .collection("rating_blood3")
    .find(
      { hid: { $gte: st, $lt: ed } },
      {
        projection: {
          hid: 1,
          _id: 0,
          "tunnel_rat.c": 1,
          "tunnel_rat.f": 1,
          "tunnel_rat.rat": 1,
          races_n: 1,
        },
      }
    )
    .toArray();
  docs = docs.map((d) => {
    let score;
    if (_.isEmpty(d) || !d?.tunnel_rat?.c) score = null;
    else {
      let { c, f, rat } = d.tunnel_rat;
      score = 1000 * (5 - c) + 100 * f_ob[f] + 10 * rat;
      // console.log(c, f, rat, score);
    }
    if (_.isNaN(score)) score = null;
    return { hid: d.hid, ...d?.tunnel_rat, score, races_n: d.races_n };
  });
  return docs;
};

let colls = ["rating_blood3", "rating_flames3", "horse_details"];
let cs = 2000;

const run = async () => {
  console.log("running ranks");
  let st = 0;
  let ed = await  cyclic_depedency.get_ed_horse();
  let ar = [];
  for (let i = st; i <= ed; i += cs) {
    let eee = Math.min(ed, i + cs);
    let docs = await get_docs(i, eee);
    ar = [...ar, ...docs];
    console.log("got docs", i, eee);
  }
  // console.log(ar[0]);

  ar = _.chain(ar)
    .compact()
    .sortBy((d) => {
      if (!d.score) return 1e14;
      return -d.score;
    })
    .value();
  let rankers = _.filter(ar, (d) => d.score !== null);
  let non_rankers = _.filter(ar, (d) => d.score == null);
  rankers = rankers.map((r, idx) => {
    return { hid: r.hid, rank: idx + 1 };
  });
  let non_rankers_hids = _.map(non_rankers, "hid");

  console.log("null_rankers", non_rankers.length);
  console.log("rankers", rankers.length);

  for (let coll of colls) {
    if (non_rankers_hids.length > 0)
      await zed_db.db
        .collection(coll)
        .updateMany(
          { hid: { $in: non_rankers_hids } },
          { $set: { rank: null } }
        );
  }

  for (let chunk of _.chunk(rankers, cs)) {
    let bulk = chunk.map(({ hid, rank }) => ({
      updateOne: {
        filter: { hid },
        update: { $set: { rank } },
      },
    }));
    bulk = _.compact(bulk);
    for (let coll of colls)
      if (!_.isEmpty(bulk)) await zed_db.db.collection(coll).bulkWrite(bulk);
    console.log("wrote", bulk.length);
  }
  console.log("completed");
};

const run_cron = async () => {
  let cron_str = "0 0 */8 * * *";
  console.log("#starting ranks_cron", cron_str, cron.validate(cron_str));
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("Next run:", c_itvl.next().toISOString());
  cron.schedule(cron_str, run);
};

const ranks = {
  run,
  run_cron,
};
module.exports = ranks;
