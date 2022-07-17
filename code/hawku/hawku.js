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
const horses_s = require("../v3/horses");
const stables_s = require("../stables/stables");

let test_mode = 0;
const name = "hawku";
const coll = "hawku";

const print_bulk_resp = (resp, msg = "") => {
  let ok = getv(resp, "ok");
  let writeErrors = getv(resp, "writeErrors")?.length || 0;
  let nInserted = getv(resp, "nInserted");
  let nUpserted = getv(resp, "nUpserted");
  let nMatched = getv(resp, "nMatched");
  let nModified = getv(resp, "nModified");
  let nRemoved = getv(resp, "nRemoved");
  let str = "";
  str += ok ? "ok " : "NOT OK";
  if (ok) {
    if (!_.isNil(nInserted)) str += nInserted + "I ";
    if (!_.isNil(nUpserted)) str += nUpserted + "U ";
    if (!_.isNil(nMatched)) str += nMatched + "M ";
    if (!_.isNil(nModified)) str += nModified + "W ";
    if (!_.isNil(nRemoved)) str += nRemoved + "R ";
    if (!_.isNil(writeErrors)) str += writeErrors + "E";
  }
  console.log(msg, str);
};

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
const track_transfers = async ([st, ed]) => {
  st = moment(st).add(-6.5, "minutes").toISOString();
  ed = moment(ed).add(0, "minutes").toISOString();
  st = fX(st);
  ed = fX(ed);
  const par = {
    asset_contract_address,
    timestamp_start: st,
    timestamp_end: ed,
    // owner_address: `0xAf1320faA9a484a4702EC16fFEC18260Cc42c3c2`,
  };
  const resp = await hget(`${base}/transfers`, par);
  let data = getv(resp, "data");
  if (_.isEmpty(data)) {
    console.log("transfers:: empty");
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

const struct_update_horse_stables_transfers = (ar) => {
  ar = _.map(ar, (e) => {
    let { transfered_at: transfer_date, token_id: hid, sender, receiver } = e;
    transfer_date = iso(transfer_date * 1000);
    return { hid, sender, receiver, transfer_date, owner: receiver };
  });
  return ar;
};
const struct_update_horse_stables_sales = (ar) => {
  ar = _.map(ar, (e) => {
    let { sold_at: transfer_date, token_id: hid, buyer, seller } = e;
    transfer_date = iso(transfer_date * 1000);
    return { hid, buyer, seller, transfer_date, owner: buyer };
  });
  return ar;
};
const update_horse_stables = async (ar) => {
  if (_.isEmpty(ar)) return;
  let now = iso();
  // ar = ar.slice(0, 1);
  // ar = _.filter(ar, (e) => e.hid == 441620);
  // console.log(ar);

  let stables = [
    ..._.map(ar, "sender"),
    ..._.map(ar, "receiver"),
    ..._.map(ar, "buyer"),
    ..._.map(ar, "seller"),
    ..._.map(ar, "owner"),
  ];
  // console.log(stables);
  let miss_stables = await stables_s.get_missing_stables_in(stables);
  if (!_.isEmpty(miss_stables)) {
    console.log("miss_stables.len", miss_stables.length);
    await stables_s.run_stables(miss_stables);
  }

  let hids = _.map(ar, "hid");
  let miss_hids = await horses_s.get_missing_hids_in(hids);
  if (!_.isEmpty(miss_hids)) {
    console.log("missing hids.len", miss_hids.length);
    await horses_s.get_only(miss_hids);
  }

  if (!_.isEmpty(ar)) {
    let resp = await zed_db.db.collection("horse_details").bulkWrite(
      ar.map((e) => {
        return {
          updateOne: {
            filter: {
              hid: e.hid,
              $or: [
                { transfer_date: { $in: [null, "iso-err"] } },
                { transfer_date: { $exists: false } },
                { transfer_date: { $lt: e.transfer_date } },
                { oid: { $ne: e.owner } },
              ],
            },
            update: {
              $set: { oid: e.owner, transfer_date: e.transfer_date },
            },
          },
        };
      })
    );
    print_bulk_resp(resp, "trans:");
  }
};

const post_track = async ({
  actives = [],
  events = [],
  sales = [],
  transfers = [],
}) => {
  let bulk = [];
  let now = fX(iso());
  console.log("actives  : ", actives.length);
  console.log("events   : ", events.length);
  console.log("sales    : ", sales.length);
  console.log("transfers: ", transfers.length);
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
    print_bulk_resp(resp, "hawku write:");
  } else console.log("nothing to write");

  let data_transfers = [
    ...struct_update_horse_stables_sales(sales),
    ...struct_update_horse_stables_transfers(transfers),
  ];
  await update_horse_stables(data_transfers);
};

const runner = async () => {
  let st = moment().add(-1, "minutes").toISOString();
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
    let transfers = await track_transfers([iso(now_st), iso(now_ed)]);
    await post_track({ actives, events, sales, transfers });
    // console.table(actives);
    // console.table(events);
    // console.table(sales);
    await cdelay(1000);
    now += off;
  }
};

const run_cron = async () => {
  const cron_str = "*/30 * * * * *";
  print_cron_details(cron_str);
  cron.schedule(cron_str, runner);
};

const clear = async () => {
  await zed_db.db.collection(coll).deleteMany({});
  console.log("cleared");
};

const fixer = async (mode, arg) => {
  let st, ed;
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
  let off = 6 * 60 * utils.mt;
  while (now < edn) {
    let now_st = nano(now);
    let now_ed = Math.min(edn, now_st + off);
    // console.log(iso(now_st), iso(now_ed));
    let sales = await track_sales([iso(now_st), iso(now_ed)]);
    let transfers = await track_transfers([iso(now_st), iso(now_ed)]);
    await post_track({ actives: [], events: [], sales, transfers });
    // console.table(actives);
    // console.table(events);
    // console.table(sales);
    await cdelay(1000);
    now += off;
  }
};

const fixer_cron = async () => {
  const cron_str = "*/15 * * * *";
  print_cron_details(cron_str);
  cron.schedule(cron_str, () => fixer("dur", [20, "minutes"]));
};

const test = async () => {
  let st = moment(1658014042 * 1000).toISOString();
  let ed = moment(1658014044 * 1000).toISOString();
  // let [st, ed] = ["2022-07-01T00:00:00.000Z", "2022-07-01T01:00:00.000Z"];
  await fixer("dates", [st, ed]);
};

const main_runner = async () => {
  let args = process.argv;
  let [_node, _cfile, arg1, arg2, arg3, arg4, arg5] = args;
  if (arg2 == "runner") await runner();
  if (arg2 == "curr_actives") await curr_actives();
  if (arg2 == "run") await run([arg3, arg4]);
  if (arg2 == "run_cron") await run_cron();
  if (arg2 == "clear") await clear();
  if (arg2 == "fixer") {
    let mode = getv(args, "3");
    let dur = parseInt(getv(args, "4") ?? 1) || 1;
    let durunit = getv(args, "3") ?? "days";
    await fixer(mode, [dur, durunit]);
  }
  if (arg2 == "fixer_cron") await fixer_cron();
  if (arg2 == "test") await test();
};

const hawku = { main_runner, hget, base, fX };
module.exports = hawku;
