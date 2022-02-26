const moment = require("moment");
const cron = require("node-cron");
const zedf = require("../utils/zedf");
const _ = require("lodash");
const utils = require("../utils/utils");
const { zed_db, zed_ch } = require("../connection/mongo_connect");
const races_base = require("./races_base");
const cyclic_depedency = require("../utils/cyclic_dependency");
const { ObjectId } = require("mongodb");

const off = 1 * 60 * utils.mt;

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
    if (_.isEmpty(rs)) {
      emp_rids.push(rid);
      continue;
    }
    let rdups_ob = _.groupBy(rs, 6);
    let rdups_n = _.keys(rdups_ob)?.length;
    if (rdups_n == 12 && rs.length == 12) continue;
    console.log("incorrect", rid, rs.length, rdups_n);
    let rdups_ids = [];
    if (rdups_n !== 12 || rs.length < 12) {
      emp_rids.push(rid);
      if (rs.length !== 0) rdups_ids = _.map(rs, "_id");
    } else {
      if (rs.length > 12) {
        rdups_ids = _.chain(rdups_ob)
          .entries()
          .map(([hid, rr]) => {
            if (rr.length == 1) return null;
            let ids = _.map(rr, "_id").slice(1);
            // console.log(hid, ids);
            return ids;
          })
          .compact()
          .flatten()
          .value();
      }
    }
    if (!_.isEmpty(rdups_ids)) dup_ids = [...dup_ids, ...rdups_ids];
  }
  return { dup_ids, emp_rids };
};

const process = async ([st, ed]) => {
  try {
    st = utils.iso(st);
    ed = utils.iso(ed);
    console.log("duplicate: %s -> %s", st, ed);
    let rdocs = await get_zed_race_format(st, ed);
    if (_.isEmpty(rdocs)) return console.log("empty rdocs");
    let { dup_ids, emp_rids } = extract_dups(rdocs);
    console.log("dups_ids", dup_ids.length);
    // console.log("dups_ids", dup_ids);
    const resp = await zed_ch.db
      .collection("zed")
      .deleteMany({ _id: { $in: dup_ids.map((id) => ObjectId(id)) } });
    console.log("deleted duplicate docs: ", resp?.deletedCount);

    console.log("emp_rids", emp_rids.length);
    console.log("emp_rids", emp_rids);
    if (!_.isEmpty(emp_rids)) await races_base.zed_race_run_rids(emp_rids);

    console.log("----");
  } catch (err) {
    console.log("err process races_duplicate", err);
  }
};

const run_dur = async ([st, ed]) => {
  try {
    st = utils.iso(st);
    ed = utils.iso(ed);
    console.log("GET DUPLICATES\n%s -> %s", st, ed);
    let now = utils.nano(st);
    let edn = utils.nano(ed);
    while (now < edn) {
      let now_st = now;
      let now_ed = Math.min(now + off, edn);
      await process([now_st, now_ed]);
      now = now_ed;
    }
    console.log("ENDED");
  } catch (err) {
    console.log("duplicates err", err.message, err);
  }
};

const runner = async () => {
  let ed = moment().subtract("10", "minutes").toISOString();
  let st = moment(new Date(ed)).subtract("5", "minutes").toISOString();
  console.log("duplicate: %s -> %s", st, ed);
  await process([st, ed]);
};

const test = async () => {};

const run_cron = async () => {
  let cron_str = "0 */5 * * * *";
  cyclic_depedency.print_cron_details(cron_str);
  cron.schedule(cron_str, runner, { scheduled: true });
};

const races_duplicate = {
  run_dur,
  runner,
  run_cron,
};

module.exports = races_duplicate;
