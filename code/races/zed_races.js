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
  const runner = async () => {
    let push_race_horses_on = 1;
    try {
      let now = Date.now();
      let from = moment(new Date(now)).subtract(10, "minutes").toISOString();
      let to = moment(new Date(now)).subtract(5, "minute").toISOString();
      await miss(from, to, push_race_horses_on);
    } catch (err) {
      console.log(err);
    }
  };
  let cron_str = "*/5 * * * *";
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("Next run:", c_itvl.next().toISOString(), "\n");
  cron.schedule(cron_str, runner, cron_conf);
};

const test = async () => {
  console.log("zed_races", "test");
  let d = "2021-12-28T21:27:08Z";
  // let from = "2021-12-28T21:27:24Z"
  let from = moment(d).subtract(5, "minutes").toISOString();
  // let to = "2021-12-28T21:32:19Z"
  let to = moment(d).add(5, "minutes").toISOString();
  console.log("from:", from);
  console.log("to  :", to);
  await races_base.zed_races_gql_runner(from, to);
};

const zed_races = {
  live,
  live_cron,
  test,
  miss,
  miss_cron,
};
module.exports = zed_races;
