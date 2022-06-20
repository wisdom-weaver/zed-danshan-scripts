const cron = require("node-cron");
const cron_parser = require("cron-parser");
const moment = require("moment");
const races_base = require("./races_base");
const races_scheduled = require("./races_scheduled");
const races_duplicate = require("./races_duplicate");
const { print_cron_details } = require("../utils/cyclic_dependency");
const { zed_races_gql_runner } = require("./races_base");
const { race_speed_adj } = require("./race_speed_adj");
const race_fixers = require("./fixers");

const cron_conf = { scheduled: true };
const race_conf_gql = { check_exists: true, durr: 1 * 60 * 60 * 1000 };
const race_conf_zrapi = { check_exists: false, durr: 2 * 60 * 1000 };

const live = async () => {
  console.log("zed_races", "live");
  let d = Date.now();
  let to = moment(d).subtract(3, "minutes").toISOString();
  let from = moment(new Date(to)).subtract(4, "minutes").toISOString();
  await races_base.zed_races_gql_runner(from, to, {
    check_exists: true,
    durr: 1 * 60 * 60 * 1000,
    push_race_horses_on: 1,
  });
};
const live_cron = async () => {
  // let cron_str = "*/30 * * * * *"; // testing
  let cron_str = "*/10 * * * * *";
  print_cron_details(cron_str);
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
    cs: 15,
    push_race_horses_on,
  });
};
const run_dur = async (from, to, push_race_horses_on = 0) => {
  console.log("zed_races", "miss");
  if (!from.endsWith("Z")) from += "Z";
  if (!to.endsWith("Z")) to += "Z";
  from = moment(new Date(from)).toISOString();
  to = moment(new Date(to)).toISOString();
  await races_base.zed_races_gql_runner(from, to, {
    check_exists: true,
    durr: 1 * 10 * 60 * 1000,
    push_race_horses_on: 1,
    cs: 15,
  });
};
const miss_cron = async () => {
  // let cron_str = "*/30 * * * * *"; // testing
  let cron_str1 = "0 */1 * * * *";
  const runner1 = async () => {
    let push_race_horses_on = 1;
    try {
      let now = Date.now();
      let from = moment(new Date(now)).subtract(30, "minutes").toISOString();
      let to = moment(new Date(now)).subtract(5, "minutes").toISOString();
      await miss(from, to, push_race_horses_on);
    } catch (err) {
      console.log(err);
    }
  };
  let cron_str2 = "*/3 * * * *";
  const runner2 = async () => {
    let push_race_horses_on = 1;
    try {
      let now = Date.now();
      let from = moment(new Date(now)).subtract(1, "hour").toISOString();
      let to = moment(new Date(now)).subtract(10, "minutes").toISOString();
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
  await races_base.zed_race_run_rids(rids);
  console.log("manual ended");
};
const test = async () => {
  let st = "2022-06-01T00:00:01Z";
  let ed = "2022-06-01T00:00:02Z";
  // let fn = races_base.zed_races_gql_runner
  // let fn = races_base.zed_races_zrapi_runner
  let fn = race_speed_adj.update_dur;
  await fn(st, ed, {
    check_exists: false,
    durr: 1 * 60 * 60 * 1000,
    push_race_horses_on: 1,
    cs: 15,
  });
};

const zed_races_sr = {
  run_dur,
  live,
  live_cron,
  miss,
  miss_cron,
  manual,
  scheduled: races_scheduled.runner,
  scheduled_cron: races_scheduled.run_cron,
  scheduled_process: races_scheduled.process,
  duplicate: races_duplicate.runner,
  duplicate_cron: races_duplicate.run_cron,
  duplicate_run_dur: races_duplicate.run_dur,
  test,
};

const main_runner = async () => {
  let [_node, _cfile, arg1, arg2, arg3, arg4, arg5] = process.argv;
  if (arg2 == "test") await zed_races_sr.test();

  if (arg2 == "run_dur") await zed_races_sr.run_dur(arg3, arg4);

  if (arg2 == "live") await zed_races_sr.live();
  if (arg2 == "live_cron") await zed_races_sr.live_cron();

  if (arg2 == "miss") await zed_races_sr.miss(arg3, arg4);
  if (arg2 == "miss_cron") await zed_races_sr.miss_cron();

  if (arg2 == "scheduled") await zed_races_sr.scheduled();
  if (arg2 == "scheduled_cron") await zed_races_sr.scheduled_cron();
  if (arg2 == "scheduled_process") await zed_races_sr.scheduled_process();

  if (arg2 == "duplicate") await zed_races_sr.duplicate();
  if (arg2 == "duplicate_cron") await zed_races_sr.duplicate_cron();
  if (arg2 == "duplicate_run_dur") {
    await zed_races_sr.duplicate_run_dur([arg3, arg4]);
  }

  if (arg2 == "manual") {
    arg3 = arg3?.split(",") ?? [];
    await zed_races_sr.manual(arg3);
  }

  if (arg2 == "fixer") {
    await race_fixers.main_runner();
  }

};
const zed_races = {
  ...zed_races_sr,
  main_runner,
};

module.exports = zed_races;
