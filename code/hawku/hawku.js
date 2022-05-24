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
const base_active = `${base}/active-listings`;
const base_sales = `${base}/sales`;
const head = { "X-HAWKU-TOKEN": process.env.hawku_token };
// timestamp_start=1651436952&timestamp_end=1653164952&offset=500&

const hget = async (base_ap, par) => {
  const se = qs.stringify(par);
  const api = `${base_ap}?${se}`;
  const resp = await fget(api, null, head).catch((err) => console.log(err));
  return resp;
};

const fX = (date) => moment(date).format("X");

const track_active = async ([st, ed]) => {
  st = fX(st);
  ed = fX(ed);
  const par = {
    asset_contract_address,
    timestamp_start: st,
    timestamp_end: ed,
  };
  const resp = await hget(base_active, par);
  let data = getv(resp, "data");
  if (_.isEmpty(data)) {
    console.log("active:: empty");
    return [];
  }
  return data;
};
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
  const resp = await hget(base_events, par);
  let data = getv(resp, "data");
  if (_.isEmpty(data)) {
    console.log("events:: empty");
    return [];
  }
  return data;
  // let upd_ar = [];
  // for (let doc of data) {
  //   let {
  //     active,
  //     asset_contract_address,
  //     currency,
  //     event_created_at,
  //     expires_at,
  //     listed_at,
  //     owner: stable,
  //     price,
  //     token_id: hid,
  //   } = doc;
  //   let ndoc = {
  //     hid,
  //     price,
  //     active,
  //     expires_at,
  //     listed_at,
  //     expires_at_iso: iso(expires_at * 1000),
  //     listed_at_iso: iso(listed_at * 1000),
  //     active,
  //   };
  //   upd_ar.push(ndoc);
  // }
  // upd_ar = _.uniqBy(upd_ar, "hid");
  // await push_bulkc(coll, upd_ar, name, "hid");
};
const track_sales = async ([st, ed]) => {
  st = fX(st);
  ed = fX(ed);
  const par = {
    asset_contract_address,
    timestamp_start: st,
    timestamp_end: ed,
  };
  const resp = await hget(base_sales, par);
  let data = getv(resp, "data");
  if (_.isEmpty(data)) {
    console.log("sales:: empty");
    return [];
  }
  return data;
};

const get_bulk_actives = (actives) => {
  let bulk = [];
  if (!_.isEmpty(actives))
    actives.map((e) => {
      let { last_updated_at, expires_at, listed_at, price, token_id: hid } = e;
      let doc = {
        hid,
        active: true,
        last_updated_at,
        expires_at,
        listed_at,
        price,
      };
      // console.log(doc);
      bulk.push({
        updateOne: {
          filter: {
            $or: [
              { hid, last_updated_at: { $lte: last_updated_at } },
              { hid, listed_at: { $lte: listed_at } },
            ],
          },
          update: { $set: doc },
          upsert: true,
        },
      });
    });
  return bulk;
};
const get_bulk_events = (events) => {
  let bulk = [];
  if (!_.isEmpty(events))
    events.map((e) => {
      let {
        active,
        event_created_at,
        last_updated_at,
        expires_at,
        listed_at,
        price,
        token_id: hid,
      } = e;
      if (!last_updated_at) last_updated_at = event_created_at;
      let doc = {
        hid,
        active,
        event_created_at,
        last_updated_at,
        expires_at,
        listed_at,
        price,
      };
      bulk.push({
        updateOne: {
          filter: {
            $or: [
              { hid, last_updated_at: { $lte: event_created_at } },
              { hid, event_created_at: { $lte: event_created_at } },
            ],
          },
          update: { $set: doc },
          upsert: false,
        },
      });
    });
  return bulk;
};

const get_bulk_sales = (sales) => {
  let bulk = [];
  if (!_.isEmpty(sales))
    sales.map((e) => {
      let { token_id: hid, sold_at } = e;
      let doc = { active: false };
      bulk.push({
        updateOne: {
          filter: {
            hid,
            listed_at: { $lte: sold_at },
            last_updated_at: { $lte: sold_at },
          },
          update: { $set: doc },
          upsert: false,
        },
      });
    });
  return bulk;
};

const post_track = async ({ actives = [], events = [], sales = [] }) => {
  let bulk = [];
  let now = fX(iso());
  console.log("actives: ", actives.length);
  console.log("events : ", events.length);
  console.log("sales  : ", sales.length);
  let bulk_actives = get_bulk_actives(actives);
  let bulk_events = get_bulk_events(events);
  let bulk_sales = get_bulk_sales(sales);
  bulk = [...bulk_actives, ...bulk_events, ...bulk_sales];

  bulk.push({
    deleteMany: { filter: { expires_at: { $ne: null, $lte: now } } },
  });

  let ref = zed_db.db.collection(coll);
  if (!_.isEmpty(bulk)) {
    let resp = await ref.bulkWrite(bulk);
    let ok = getv(resp, "ok");
    let writeErrors = getv(resp, "writeErrors")?.length || 0;
    let nInserted = getv(resp, "nInserted");
    let nUpserted = getv(resp, "nUpserted");
    let nMatched = getv(resp, "nMatched");
    let nModified = getv(resp, "nModified");
    let nRemoved = getv(resp, "nRemoved");
    console.log({
      ok,
      writeErrors,
      nInserted,
      nUpserted,
      nMatched,
      nModified,
      nRemoved,
    });
  } else console.log("nothing to write");
};

const runner = async () => {
  let st = moment().add(-10, "minutes").toISOString();
  let ed = moment().add(0, "minutes").toISOString();
  await run([st, ed]);
};

const curr_actives = async () => {};

const run = async ([st, ed]) => {
  console.log(iso(st), "->", iso(ed));
  let now = nano(st);
  let edn = nano(ed);
  let off = 60 * utils.mt;
  while (now < edn) {
    let now_st = nano(now);
    let now_ed = Math.min(edn, now_st + off);
    // console.log(iso(now_st), iso(now_ed));
    let actives = await track_active([iso(now_st), iso(now_ed)]);
    let events = await track_events([iso(now_st), iso(now_ed)]);
    let sales = await track_sales([iso(now_st), iso(now_ed)]);
    await post_track({ actives, events, sales });
    // console.table(actives);
    // console.table(events);
    // console.table(sales);
    await cdelay(1000);
    now += off;
  }
};

const run_cron = async () => {
  const cron_str = "*/10 * * * *";
  print_cron_details(cron_str);
  cron.schedule(cron_str, runner);
};

const clear = async () => {
  await zed_db.db.collection(coll).deleteMany({});
  console.log("cleared");
};

const main_runner = async () => {
  let args = process.argv;
  let [_node, _cfile, arg1, arg2, arg3, arg4, arg5] = args;
  if (arg2 == "runner") await runner();
  if (arg2 == "curr_actives") await curr_actives();
  if (arg2 == "run") await run([arg3, arg4]);
  if (arg2 == "run_cron") await run_cron();
  if (arg2 == "clear") await clear();
};

const hawku = { main_runner };
module.exports = hawku;
