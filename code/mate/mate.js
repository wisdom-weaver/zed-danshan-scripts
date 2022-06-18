const _ = require("lodash");
const qs = require("query-string");
const {
  get_date_range_fromto,
  print_cron_details,
  z_mi_mx,
} = require("../utils/cyclic_dependency");
const { fget } = require("../utils/fetch");
const { nano, iso, cdelay, geno } = require("../utils/utils");
const moment = require("moment");
const { zed_db } = require("../connection/mongo_connect");
const zedf = require("../utils/zedf");
const cron = require("node-cron");
const { push_bulkc } = require("../utils/bulk");
const { options } = require("../utils/options");
const { max } = require("lodash");

let ref;
const coll = "studs";
const name = "studs";

let test_mode = 0;
let runnable = 1;

const base_api = `https://api.zed.run/api/v1/stud/horses`;
const mapi = ({ offset = 0, gen = [], breed_type, bloodline }) => {
  let se = qs.stringify(
    { offset, gen, breed_type, bloodline },
    { arrayFormat: "bracket" }
  );
  return `${base_api}?${se}`;
};

const get_hid = (inpu) => {
  let str;
  if (inpu.length == 650) str = inpu.substring(458, 458 + 8);
  else if (inpu.length == 74) str = inpu.substring(74 - 8);
  else return null;
  let hid = parseInt(str, 16);
  return hid;
};

const get_stud_transfer_type = (inpu) => {
  if (inpu.length == 650) return "in";
  else if (inpu.length == 74) return "out";
  else return null;
};

const get_mating_price = (inp) => {
  try {
    return parseInt(inp.slice(380, 380 + 14), 16) / 1e18;
  } catch (err) {
    return null;
  }
};

const extract_raw = async ({
  from_date = undefined,
  to_date = undefined,
  cursor = undefined,
  limit = undefined,
}) => {
  let se = qs.stringify({
    chain: "polygon",
    from_date,
    to_date,
    cursor,
    limit,
  });
  let api = `https://deep-index.moralis.io/api/v2/0x7adbced399630dd11a97f2e2dc206911167071ae?${se}`;
  let resp = await fget(api, null, {
    "X-API-Key": process.env.moralis_api_key,
  });
  return resp;
};

const extract = async ([st, ed]) => {
  let ar = [];
  let cursor;
  let n = 1;
  do {
    let from_date = st;
    let to_date = ed;
    let resp = await extract_raw({ from_date, to_date, cursor });
    const { total, page_size, cursor: cn, page } = resp;
    cursor = cn;
    if (!cursor) break;
    ar.push(resp?.result);
  } while (n--);
  ar = _.flatten(ar);
  return ar;
};

const get_hdocs = async (hids) => {
  let hdocs = [];
  for (let chu of _.chunk(hids, 10)) {
    let ea = await Promise.all(
      chu.map((h) => zedf.horse(h).then((d) => ({ ...d, hid: h })))
    );
    ea = _.compact(ea);
    hdocs.push(ea);
  }
  hdocs = _.flatten(hdocs);
  hdocs = _.keyBy(hdocs, "hid");
  return hdocs;
};

const struct_hdoc = (hdoc, type, in_date) => {
  let {
    hid,
    horse_type,
    is_in_stud,
    breeding_cycle_reset,
    breeding_counter,
    mating_price,
  } = hdoc;
  let gender =
    (["Stallion", "Colt"].includes(horse_type) && "M") ||
    (["Filly", "Mare"].includes(horse_type) && "F") ||
    null;
  if (in_date) {
    let diff = moment().diff(moment(in_date), "minutes");
    if (_.inRange(diff, 0, 10.1)) {
      if (type == "in") is_in_stud = true;
      if (type == "out") is_in_stud = false;
    }
  }
  if (type == "cur") is_in_stud = true;
  if (mating_price) mating_price = parseFloat(mating_price) / 1e18;
  return {
    hid,
    gender,
    horse_type,
    stud: is_in_stud,
    mating_price,
    breeding_counter,
    breeding_cycle_reset,
  };
};

const run_in = async (ar) => {
  console.log("#run_in", ar.length);
  let hids = _.map(ar, "hid");
  // console.log(hids);
  let hdocs = await get_hdocs(hids);
  ar = _.map(ar, (e) => {
    let hid = e.hid;
    let hdoc = hdocs[hid];
    if (!hdoc) return null;
    let hdocf = struct_hdoc(hdoc, "in", e.date);
    return { hid, ...e, ...hdocf };
  });
  ar = _.compact(ar);
  // console.table(ar);
  await push_bulkc(coll, ar, name, "hid");
  await cdelay(2000);
};
const run_out = async (ar) => {
  console.log("#run_out", ar.length);
  let hids = _.map(ar, "hid");
  // console.log(hids);
  let hdocs = await get_hdocs(hids);
  ar = _.map(ar, (e) => {
    let hid = e.hid;
    let hdoc = hdocs[hid];
    if (!hdoc) return null;
    let hdocf = struct_hdoc(hdoc, "out", e.date);
    return { ...e, ...hdocf, hid };
  });
  ar = _.compact(ar);
  // console.table(ar);
  await push_bulkc(coll, ar, name, "hid");
  await cdelay(2000);
};
const rem_expired = async () => {
  let now = iso();
  console.log("deleting expired before", now);
  await ref.deleteMany({
    $or: [{ breeding_cycle_reset: { $lte: now } }, { stud: false }],
  });
  await cdelay(2000);
};

const structure_txns_resp = (ar) => {
  if (_.isEmpty(ar)) return [];
  let stru = ar.map((e) => {
    let { hash, from_address, to_address, input, block_timestamp } = e;
    let type = get_stud_transfer_type(input);
    if (!type) return null;
    let hid = get_hid(input);
    let mating_price = get_mating_price(input);
    return {
      type,
      stable: from_address,
      hid,
      date: block_timestamp,
      hash,
      mating_price,
    };
  });
  stru = _.compact(stru);
  return stru;
};

const run = async ([st, ed]) => {
  console.log("### mate run");
  console.log(`getting .. ${st} - ${ed}`);
  let ar = await extract([st, ed]);
  ar = structure_txns_resp(ar);
  console.table(ar);
  console.log("found:", ar?.length);
  ar = _.groupBy(ar, "type");
  await run_in(ar?.in);
  await run_out(ar?.out);
  await rem_expired();
};

const runner = async () => {
  let [st, ed] = get_date_range_fromto(-10, "minutes", 0, "minutes");
  await run([st, ed]);
};

const run_cron = async () => {
  let cron_str = "0 * * * * *";
  print_cron_details(cron_str);
  cron.schedule(cron_str, runner, { scheduled: true });
};

const get_current_in_catraw = async (ob) => {
  let { bloodline, breed_type, z } = ob;
  let offset = 0;
  let ar = [];
  do {
    let api = mapi({ ...ob, offset, gen: [z, z] });
    // console.log(api);
    let resp = await zedf.get(api);
    if (_.isEmpty(resp)) break;
    offset += resp.length;
    ar.push(resp);
    cdelay(500);
  } while (true);
  ar = _.flatten(ar);
  return ar;
};
const get_current_in_cat = async (ob) => {
  let { bloodline, breed_type, z } = ob;
  let now = iso();
  let ar = await get_current_in_catraw({ bloodline, breed_type, z });
  console.log(`Z${z}-${bloodline}-${breed_type}`, ar.length);
  ar = ar.map((e) => ({ ...e, hid: e.horse_id }));
  ar = ar.map((e) => struct_hdoc(e, "cur"));
  ar = ar.map((e) => ({ ...e, date: now, type: "cur" }));
  // console.table(ar);
  await run_in(ar);
};

const get_current = async (a3 = 3) => {
  let ks = [];
  let cs = parseFloat(a3) || 3;
  if (!cs || _.isNaN(cs)) cs = 3;
  for (let bl of options.bloodline)
    for (let bt of options.breed_type) {
      let [mi = 1, mx = 268] = z_mi_mx[`${bl}-${bt}`];
      for (let z = mi; z <= mx; z++) {
        ks.push({ bloodline: bl, breed_type: bt, z });
      }
    }
  for (let chu of _.chunk(ks, cs)) {
    await Promise.all(chu.map((e) => get_current_in_cat(e)));
  }
  console.log("done");
};

const clear = async () => {
  await ref.deleteMany({});
};

const main_runner = async () => {
  ref = await zed_db.db.collection(coll);
  let args = process.argv;
  let [n, f, a1, a2, a3, a4] = args;
  if (args.includes("test")) test_mode = 1;
  if (!runnable) return;
  if (a2 == "run") await run([a3, a4]);
  if (a2 == "runner") await runner();
  if (a2 == "run_cron") await run_cron();
  if (a2 == "get_current") await get_current();
  if (a2 == "clear") await clear();
};

const mate = { main_runner };
module.exports = mate;
