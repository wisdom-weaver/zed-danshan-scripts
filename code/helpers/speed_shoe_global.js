const _ = require("lodash");
const { zed_db, zed_ch } = require("../connection/mongo_connect");
const {
  print_cron_details,
  get_date_range_fromto,
} = require("../utils/cyclic_dependency");
const {
  getv,
  calc_median,
  cron_conf,
  nano,
  iso,
  mt,
} = require("../utils/utils");
const cron = require("node-cron");
const dists = [1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600];

let off = 60 * mt;

const generate = async () => {
  let [st, ed] = get_date_range_fromto(-1, "days", 0, "minutes");
  console.log([st, ed]);
  let glob = {};
  for (let dist of dists) {
    let ntimes = [];

    for (let now = nano(st); now < nano(ed); ) {
      let now_ed = now + off;
      try {
        let doc = await zed_ch.db
          .collection("zed")
          .aggregate([
            {
              $match: {
                2: {
                  $gte: iso(now),
                  $lte: iso(now_ed),
                },
                1: dist,
                // 6: hid,
              },
            },
            {
              $project: {
                23: 1,
              },
            },
          ])
          .toArray();
        let ar = _.map(doc, "23");

        console.log(dist, iso(now), iso(now_ed), ar.length);
        ntimes.push(ar);
      } catch (err) {
        console.log(err.message);
      }
      now = Math.min(nano(ed), now_ed + 1);
    }
    ntimes = _.flatten(ntimes);

    let ob;
    if (_.isEmpty(ntimes)) {
      ob = { dist, count: 0, mi: null, mx: null, med: null };
    } else {
      let a = _.filter(ntimes, (e) => ![0, null, undefined, NaN].includes(e));
      ob = {
        n: parseInt(a.length),
        count: parseInt(a.length / 12),
        mi: _.min(a),
        mx: _.max(a),
        med: calc_median(a),
      };
    }
    console.log(dist, ob);
    glob[dist] = ob;
  }
  console.table(glob);
  let ndoc = { id: "speed_shoe_global", glob };
  await zed_db.db
    .collection("requirements")
    .updateOne({ id: ndoc.id }, { $set: ndoc }, { upsert: true });
  console.log("done");
};

const run_cron = () => {
  let cron_str = "0 0 0 * * *";
  print_cron_details(cron_str);
  // cron.schedule(cron_str, generate, cron_conf);
};
const main_runner = async () => {
  let [_node, _cfile, v, arg1, arg2, arg3, arg4, arg5] = process.argv;
  console.log(arg1);
  if (arg2 == "cron") await run_cron();
  if (arg2 == "run") await generate();
};
const speed_shoe = {
  main_runner,
};
module.exports = speed_shoe;
