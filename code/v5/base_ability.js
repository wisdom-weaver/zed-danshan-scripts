const { get_races_of_hid } = require("../utils/cyclic_dependency");
const _ = require("lodash");
const bulk = require("../utils/bulk");
const { zed_db } = require("../connection/mongo_connect");
const { options } = require("../utils/options");
const { geno, dec, get_fee_tag, getv } = require("../utils/utils");
const race_utils = require("../utils/race_utils");
const cyclic_depedency = require("../utils/cyclic_dependency");
const { knex_conn } = require("../connection/knex_connect");
const { dist_factor } = require("./speed");

const coll = "base_ability5";
const name = "base_ability v5";
let cs = 200;
let test_mode = 0;

const get_knex_speeds = async ({ hid }) => {
  let ar = await knex_conn
    .select(
      knex_conn.raw(
        [1400, 1600, 1800]
          .map(
            (dist) =>
              `
              (
                COALESCE(cast(stats->'${dist}'->'firsts' as int),0) +
                COALESCE(cast(stats->'${dist}'->'seconds' as int),0) +
                COALESCE(cast(stats->'${dist}'->'thirds' as int),0) +
                COALESCE(cast(stats->'${dist}'->'fourths' as int),0) +
                COALESCE(cast(stats->'${dist}'->'other' as int),0)
              ) as n${dist},
              stats->'${dist}'->'median_speed' as spov_${dist} `
          )
          .join(",")
      )
    )
    .from("horses")
    .where(knex_conn.raw(`id=${hid}`));
  ar = getv(ar, "0");
  return ar;
};

const isnil = (e) => [0, null, undefined, NaN].includes(e);
const dfact = {
  1600: 1,
  1400: 0.9940862274,
  1800: 1.000224118,
};

const filt_req_speed = (speed_ob) => {
  console.table(speed_ob);
  let {
    n1400,
    n1600,
    n1800,

    spov_1400,
    spov_1600,
    spov_1800,
  } = speed_ob;
  let n = n1400 + n1600 + n1800;
  if (n < 15) return null;
  let sp =
    (n1400 * dist_factor[1400] * spov_1400 +
      n1600 * dist_factor[1600] * spov_1600 +
      n1800 * dist_factor[1600] * spov_1800) /
    n;
  // console.log({ n, sp });
  return { sp, k: "spov", n };
  return null;
};

const calc_scale = (e, a, b) => {
  if (e >= b) return 10;
  if (e <= a) return 0;
  return ((e - a) / (b - a)) * 10;
};

const calc = async ({ hid }) => {
  let speed_ob = await get_knex_speeds({ hid });
  if (_.isEmpty(speed_ob)) {
    console.log(hid, "empty speed ob");
    return null;
  }
  let filt_sp = filt_req_speed(speed_ob);
  if (_.isEmpty(filt_sp)) {
    console.log(hid, "null speed_init");
    return null;
  }
  console.log(filt_sp);
  let { sp: speed_init, k, n } = filt_sp;
  let speed_rat = 1.45 * speed_init;
  let ba = calc_scale(speed_rat, 88, 92);
  console.log(hid, { k, n, speed_init, speed_rat, ba });
  return null;
};

const generate = async (hid) => {
  hid = parseInt(hid);
  let ob = await calc({ hid });
  if (test_mode) console.log(ob);
  return ob;
};

const all = async () => bulk.run_bulk_all(name, generate, coll, cs, test_mode);
const only = async (hids) =>
  bulk.run_bulk_only(name, generate, coll, hids, cs, test_mode);
const range = async (st, ed) =>
  bulk.run_bulk_range(name, generate, coll, st, ed, cs, test_mode);

const test = async (hids) => {};

const fixer = async () => {};

const main_runner = async () => {
  let [_node, _cfile, v, arg1, arg2, arg3, arg4, arg5] = process.argv;
  console.log("# base_ability");
  if (arg2 == "all") await all();
  if (arg2 == "only") {
    let conf = JSON.parse(arg3) || [];
    console.log("only conf", conf);
    await only(conf);
  }
  if (arg2 == "range") await range(jparse(arg3));

  if (arg2 == "fixer") await fixer();

  if (arg2 == "test") {
    let conf = JSON.parse(arg3) || {};
    await test(conf);
  }
};

const base_ability = {
  main_runner,
  calc,
  generate,
};
module.exports = base_ability;
