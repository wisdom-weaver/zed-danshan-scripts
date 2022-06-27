const _ = require("lodash");
const moment = require("moment");
const cron = require("node-cron");
const { zed_db } = require("../connection/mongo_connect");
const bulk = require("../utils/bulk");
const { get_date_range_fromto } = require("../utils/cyclic_dependency");
const { nano, cdelay, iso } = require("../utils/utils");
const { fX } = require("./hawku");
const hawku = require("./hawku");

let off = 1 * 60 * 1000;

const get_owner_stuff = () => {


};

const bulk_transfers_write = async (data) => {
  await bulk.push_bulkc("transfers", data, "transfers", "txhash");
};

const get_transfers = async ([st, ed]) => {
  console.log([st, ed]);
  let baseap = hawku.base + "/transfers";

  let tot = 0;
  let offset = 0;
  do {
    let par = {
      asset_contract_address: "0x67f4732266c7300cca593c814d46bee72e40659f",
      timestamp_start: hawku.fX(st),
      timestamp_end: hawku.fX(ed),
      offset,
    };
    let resp = await hawku.hget(baseap, par);
    let data = await resp.data;
    if (_.isEmpty(data)) break;
    data = data.map((e) => {
      let {
        sender,
        receiver,
        token_id: hid,
        transfered_at,
        transaction_hash: txhash,
      } = e;
      let date = iso(transfered_at * 1000);
      return { hid, txhash, sender, receiver, date };
    });
    console.log(data[0].date);
    await bulk_transfers_write(data);
    console.log(data.length);
    offset += data.length;
    tot += data.length;
  } while (true);
  console.log("done total", tot);
};

const test = async () => {
  let [st, ed] = get_date_range_fromto(-5, "days", 0, "minutes");
  // let [st, ed] = ["2022-06-25T18:00:00.000Z", "2022-06-25T18:10:00.000Z"];
  let resp = await get_transfers([st, ed]);
  console.log(resp);
};

const clear = async () => {
  await zed_db.db.collection("transfers").deleteMany({});
  console.log("done");
};

const main_runner = async () => {
  let args = process.argv;
  let [_node, _cfile, arg1, arg2, arg3, arg4, arg5] = args;
  if (arg2 == "test") await test();
  if (arg2 == "clear") await clear();
};

const transfers = {
  main_runner,
};
module.exports = transfers;
