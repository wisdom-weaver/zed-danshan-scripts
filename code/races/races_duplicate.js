const moment = require("moment");
const cron = require("node-cron");
const zedf = require("../utils/zedf");
const _ = require("lodash");
const utils = require("../utils/utils");
const { zed_db, zed_ch } = require("../connection/mongo_connect");
const races_base = require("./races_base");
const cyclic_depedency = require("../utils/cyclic_dependency");

const get_zed_race_format = async (from, to) => {
  try {
    from = utils.iso(from);
    to = utils.iso(to);
    let docs =
      (await zed_ch.db
        .collection("zed")
        .find(
          { 2: { $gte: from, $lte: to } },
          { projection: { _id: 1, 4: 1, 6: 1 } }
        )
        .toArray()) || [];
    if (_.isEmpty(docs)) return null;
    docs = _.groupBy(docs, "4");
    return docs;
  } catch (err) {
    console.log("err", err);
    return [];
  }
};
const extract_dups = (races) => {
  let dup_ids = [];
  let emp_rids = [];
  if (_.isEmpty(races)) return { dup_ids, emp_rids };
  for (let [rid, rs] of _.entries(races)) {
    if (_.isEmpty(rs)) continue;
    if (rs.length !== 12) {
      console.log("incorrect", rid, rs.length);
      if (rs.length > 12) {
        let dups = _.chain(rs)
          .groupBy(4)
          .entries()
          .map(([hid, rr]) => {
            if (rr.length == 1) return null;
            return _.map(rr, "_id").slice(1);
          })
          .compact()
          .flatten()
          .value();
        if (!_.isEmpty(dups)) dup_ids = [...dup_ids, ...dups];
      } else {
        emp_rids.push(rid);
      }
    }
  }
  return { dup_ids, emp_rids };
};

const process = async ([st, ed]) => {
  try {
    st = utils.iso(st);
    ed = utils.iso(ed);
    let rdocs = await get_zed_race_format(st, ed);
    if (_.isEmpty(rdocs)) return console.log("empty rdocs");
    let { dup_ids, emp_rids } = extract_dups(rdocs);
    console.log("dups_ids", dup_ids.length);
    console.log("emp_rids", emp_rids.length);
  } catch (err) {
    console.log("err process races_duplicate", err);
  }
};

const runner = async () => {
  let ed = moment().subtract("2", "minutes").toISOString();
  let st = moment(new Date(ed)).subtract("5", "minutes").toISOString();
  console.log("duplicate: %s -> %s", st, ed);
  await process([st, ed]);
};

const test = async () => {
  // runner();
  run_cron();
};

const run_cron = async () => {
  let cron_str = "0 */5 * * * *";
  cyclic_depedency.print_cron_details(cron_str);
  cron.schedule(cron_str, runner, { scheduled: true });
};

const races_duplicate = {
  runner,
  run_cron,
};

module.exports = races_duplicate;
