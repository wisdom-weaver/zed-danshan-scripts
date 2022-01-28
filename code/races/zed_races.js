const cron = require("node-cron");
const cron_parser = require("cron-parser");
const moment = require("moment");
const races_base = require("./races_base");

const cron_conf = { scheduled: true };
const race_conf_gql = { check_exists: true, durr: 1 * 60 * 60 * 1000 };
const race_conf_zrapi = { check_exists: false, durr: 2 * 60 * 1000 };

const live_offset_to_s = 60 * 2;
const live_offset_from_s = 17 + live_offset_to_s;

const live = async () => {
  console.log("zed_races", "live");
  let d = Date.now();
  let from = moment(d).subtract(live_offset_from_s, "seconds").toISOString();
  let to = moment(d).subtract(live_offset_to_s, "seconds").toISOString();
  await races_base.zed_races_gql_runner(from, to, {
    check_exists: false,
    durr: 1 * 60 * 60 * 1000,
    push_race_horses_on: 1,
  });
};
const live_cron = async () => {
  let cron_str = "*/15 * * * * *";
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("Next run:", c_itvl.next().toISOString(), "\n");
  cron.schedule(cron_str, live, cron_conf);
};

const miss = async (from, to, push_race_horses_on = 0) => {
  console.log("zed_races", "miss");
  if (!from.endsWith("Z")) from += "Z";
  if (!to.endsWith("Z")) to += "Z";
  from = moment(new Date(from)).toISOString();
  to = moment(new Date(to)).toISOString();
  await races_base.zed_races_zrapi_runner(from, to, {
    check_exists: true,
    durr: 1 * 60 * 60 * 1000,
    cs: 10,
    push_race_horses_on,
  });
};
const miss_cron = async () => {
  let cron_str1 = "*/1 * * * *";
  const runner1 = async () => {
    let push_race_horses_on = 1;
    try {
      let now = Date.now();
      let from = moment(new Date(now)).subtract(10, "minutes").toISOString();
      let to = moment(new Date(now)).toISOString();
      await miss(from, to, push_race_horses_on);
    } catch (err) {
      console.log(err);
    }
  };
  let cron_str2 = "*/5 * * * *";
  const runner2 = async () => {
    let push_race_horses_on = 1;
    try {
      let now = Date.now();
      let from = moment(new Date(now)).subtract(1, "hour").toISOString();
      let to = moment(new Date(now)).toISOString();
      await miss(from, to, push_race_horses_on);
    } catch (err) {
      console.log(err);
    }
  };
  const c_itvl1 = cron_parser.parseExpression(cron_str1);
  console.log("Next run:", c_itvl1.next().toISOString());
  cron.schedule(cron_str1, runner1, cron_conf);

  const c_itvl2 = cron_parser.parseExpression(cron_str2);
  console.log("Next run:", c_itvl2.next().toISOString());
  cron.schedule(cron_str2, runner2, cron_conf);
};

const manual = async (rids) => {
  console.log("zed_races", "manual");
  await races_base.zed_race_rids(rids);
  console.log("manual ended");
};
const test = async () => {};

const zed_races = {
  live,
  live_cron,
  test,
  miss,
  miss_cron,
  manual,
};
module.exports = zed_races;
