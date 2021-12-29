const _ = require("lodash");
const { zed_ch, zed_db } = require("../connection/mongo_connect");

const test_mode = 0;
const cs = 100;

const calc = async ({ hid }) => {};
const generate = async (hid) => {

};

// const all = async () => bulk.run_bulk_all(name, generate, coll, cs, test_mode);
// const only = async (hids) =>
  // bulk.run_bulk_only(name, generate, coll, hids, cs, test_mode);
// const range = async (st, ed) =>
  // bulk.run_bulk_range(name, generate, coll, st, ed, cs, test_mode);

const test = (hids) => {
  test_mode = 1;
  for (let hid of hids) {
    let ob = await generate(hid);
    console.log(ob);
  }
};

const odds_generator = { calc, generate, all, only, range, test };
module.exports = odds_generator;
