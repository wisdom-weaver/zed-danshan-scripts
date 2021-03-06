const _ = require("lodash");
const { zed_db, zed_ch } = require("../connection/mongo_connect");
const utils = require("../utils/utils");
const moment = require("moment");
const { getv, nano, iso, cdelay } = require("../utils/utils");
const { print_cron_details } = require("../utils/cyclic_dependency");
const cron = require("node-cron");
const red = require("../connection/redis");
const axios = require("axios");
const zedf = require("../utils/zedf");
const bulk = require("../utils/bulk");

let test_mode = 0;
const zed_gql = "https://zed-ql.zed.run/graphql/getRaceResults";
const zed_secret_key = process.env.zed_secret_key;
const get_zed_raw_data = async (from, to, cursor, lim = 100) => {
  try {
    let arr = [];
    let json = {};
    let headers = {
      "Content-Type": "application/json",
      Authorization: `Bearer ${zed_secret_key}`,
      Cookie:
        "__cf_bm=tEjKpZDvjFiRn.tUIx1TbiSLPLfAmtzyUWnQo6VHP7I-1636398985-0-ARRsf8lodPXym9lS5lNpyUbf3Hz4a6TJovc1m+sRottgtEN/MoOiOpoNcpW4I0wcA0q4VwQdEKi7Q8VeW8amlWA=",
    };
    let payload = {
      query: `query ($input: GetRaceResultsInput, $before: String, $after: String, $first: Int, $last: Int) {
  getRaceResults(before: $before, after: $after, first: $first, last: $last, input: $input) {
    edges {
      cursor
      node {
        horses {
          horseId
        }
      }
    }
    pageInfo: page_info {
      startCursor: start_cursor
      endCursor: end_cursor
      hasNextPage: has_next_page
      hasPreviousPage: has_previous_page
    }
  }
}`,
      variables: {
        first: lim,
        after: cursor,
        input: {
          dates: {
            from: from,
            to: to,
          },
        },
      },
    };

    let axios_config = {
      method: "post",
      url: zed_gql,
      headers: headers,
      data: JSON.stringify(payload),
    };
    let result = await axios(axios_config);
    // console.log(result.data.data);
    let data = result?.data?.data?.getRaceResults || {};
    let edges = data?.edges || [];
    let pageInfo = data?.pageInfo || {};
    let racesData = [];
    for (let e of edges) {
      // let rid = getv(e, "node.raceId");
      // let date = getv(e, "node.startTime");
      let ar = getv(e, "node.horses");
      // console.log({ rid, date });
      for (let h of ar) {
        let { horseId: hid } = h;
        // console.log({ hid, tc });
        racesData.push({ hid });
      }
    }
    return { racesData, pageInfo };
  } catch (err) {
    console.log(err.message);
    throw new Error(err.message);
  }
};

const watch_classes = async (rdata) => {
  let hids = _.map(rdata, "hid");
  hids = _.uniq(hids);
  console.log("getting", hids.length);
  for (let chu of _.chunk(hids, 50)) {
    try {
      let eaar = _.chunk(chu, 10);
      let ar = await Promise.all(eaar.map((hs) => zedf.horses(hs)));
      ar = _.flatten(ar);
      ar = _.map(ar, (e) => {
        return { hid: e.horse_id, tc: e.class };
      });
      await bulk.push_bulkc("horse_details", ar, "class_update", "hid");
      await cdelay(1000);
    } catch (err) {
      console.log(err.message);
    }
  }
};

const run = async ([st, ed]) => {
  console.log(iso(st), "->", iso(ed));
  let now = nano(st);
  let edn = nano(ed);
  let off = 60 * utils.mt;
  let cursor = null;
  do {
    try {
      let resp = await get_zed_raw_data(st, ed, cursor, 50);
      cursor = getv(resp, "pageInfo.endCursor");
      console.log("rdata:", resp.racesData?.length);
      await watch_classes(resp.racesData);
      let hasNextPage = getv(resp, "pageInfo.hasNextPage");
      if (!hasNextPage) break;
      await cdelay(2000);
    } catch (err) {
      console.log("err", err.message);
    }
  } while (true);
};

const runner = async () => {
  console.log("runner started");
  let st = moment().add(-5, "minutes").toISOString();
  let ed = moment().add(-4, "minutes").toISOString();
  await run([st, ed]);
};

const run_cron = async () => {
  const cron_str = "*/50 * * * * *";
  print_cron_details(cron_str);
  cron.schedule(cron_str, runner);
};

const fixer = async (mode, arg) => {
  let st, ed;
  console.log("fixer ", mode, arg);
  if (mode == "dur") {
    let [dur, durunit] = arg;
    console.log({ dur, durunit });
    ed = moment().toISOString();
    st = moment().subtract(dur, durunit).toISOString();
  } else if (mode == "dates") {
    st = moment(arg[0]).toISOString();
    ed = moment(arg[1]).toISOString();
  }
  console.log(iso(st), "->", iso(ed));
  let now = nano(st);
  let edn = nano(ed);
};

const fixer_cron = async () => {
  const cron_str = "*/15 * * * *";
  print_cron_details(cron_str);
  cron.schedule(cron_str, () => fixer("dur", [20, "minutes"]));
};

const test = async () => {
  runner();
};

const main_runner = async () => {
  console.log("--hclass");
  let args = process.argv;
  let [_node, _cfile, arg1, arg2, arg3, arg4, arg5] = args;
  if (arg2 == "runner") await runner();
  if (arg2 == "run") await run([arg3, arg4]);
  if (arg2 == "run_cron") await run_cron();
  if (arg2 == "fixer") {
    console.log(args);
    let mode = getv(args, "4");
    let dur = parseInt(getv(args, "5") ?? 1) || 1;
    let durunit = getv(args, "6") ?? "days";
    await fixer(mode, [dur, durunit]);
  }
  if (arg2 == "fixer_cron") await fixer_cron();
  if (arg2 == "test") await test();
};

const hclass = { main_runner };
module.exports = hclass;
