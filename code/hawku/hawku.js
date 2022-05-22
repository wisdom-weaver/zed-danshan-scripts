const _ = require("lodash");
const { zed_db, zed_ch } = require("../connection/mongo_connect");
const global_req = require("../global_req/global_req");
const bulk = require("../utils/bulk");
const cyclic_depedency = require("../utils/cyclic_dependency");
const utils = require("../utils/utils");
const moment = require("moment");
const qs = require("query-string");
const { fget } = require("../utils/fetch");
const { getv, nano, iso, cdelay } = require("../utils/utils");
const { push_bulkc } = require("../utils/bulk");
const { print_cron_details } = require("../utils/cyclic_dependency");
const cron = require("node-cron");

let test_mode = 0;
const name = "hawku";
const coll = "hawku";

const base = `https://api.hawku.com/api/v1/marketplace`;
const asset_contract_address = `0x67F4732266C7300cca593C814d46bee72e40659F`;
const base_events = `${base}/listing-events`;
const head = { "X-HAWKU-TOKEN": process.env.hawku_token };
// timestamp_start=1651436952&timestamp_end=1653164952&offset=500&

const track_events = async ([st, ed]) => {
  console.log("tracking", st, ed);
  st = moment(st).format("X");
  ed = moment(ed).format("X");
  const par = {
    asset_contract_address,
    timestamp_start: st,
    timestamp_end: ed,
  };
  // console.log(par);
  // console.log(head);
  const se = qs.stringify(par);
  const api = `${base_events}?${se}`;
  const resp = await fget(api, null, head).catch((err) => console.log(err));
  let data = getv(resp, "data");
  let upd_ar = [];
  let del_hids = [];
  for (let doc of data) {
    let {
      active,
      asset_contract_address,
      currency,
      event_created_at,
      expires_at,
      listed_at,
      owner: stable,
      price,
      token_id: hid,
    } = doc;
    let ndoc = {
      hid,
      price,
      active,
      expires_at,
      listed_at,
      expires_at_iso: iso(expires_at * 1000),
      listed_at_iso: iso(listed_at * 1000),
      active,
    };
    upd_ar.push(ndoc);
  }
  upd_ar = _.uniqBy(upd_ar, "hid");
  await push_bulkc(coll, upd_ar, name, "hid");

  let now = parseInt(moment().format("X"));
  let expired = await zed_db.db.collection(coll).deleteMany({
    $or: [
      { expires_at: { $lte: now } },
      { active: false, listed_at: { $gte: now } },
    ],
  });
};

const runner = async () => {
  let st = moment().add(-10, "minutes").toISOString();
  let ed = moment().add(0, "minutes").toISOString();
  await run([st, ed]);
};

const run = async ([st, ed]) => {
  console.log(iso(st), "->", iso(ed));
  let now = nano(st);
  let edn = nano(ed);
  let off = 60 * utils.mt;
  while (now < edn) {
    let now_st = nano(now);
    let now_ed = Math.min(edn, now_st + off);
    // console.log(iso(now_st), iso(now_ed));
    await track_events([iso(now_st), iso(now_ed)]);
    await cdelay(1000);
    now += off;
  }
};

const run_cron = async () => {
  const cron_str = "*/10 * * * *";
  print_cron_details(cron_str);
  cron.schedule(cron_str, runner);
};

const main_runner = async () => {
  let args = process.argv;
  let [_node, _cfile, arg1, arg2, arg3, arg4, arg5] = args;
  if (arg2 == "runner") await runner();
  if (arg2 == "run") await run([arg3, arg4]);
  if (arg2 == "run_cron") await run_cron();
};

const hawku = { main_runner };
module.exports = hawku;
