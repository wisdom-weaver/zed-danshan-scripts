const _ = require("lodash");
const { MongoClient } = require("mongodb");
const mongoose = require("mongoose");
const fetch = require("node-fetch");
const { from_ch_zed_collection } = require("./index-odds-generator");
require("dotenv").config();

// let zed_db = mongoose.connection;
// const get_first_raceid = async (hid) => {
//   let r1 = await zed_db.db.collection("zed3").findOne({ 6: hid });
//   if (r1 && !_.isEmpty(r1)) return r1["4"];
//   else return null;
// };
// const get_horses_in_race = async (rid) => {
//   rid = rid.toString();
//   let ob = await from_ch_zed_collection({ 4: rid });
//   console.log({ ob });

//   if (_.isEmpty(ob)) return null;
//   ob = _.chain(ob).keyBy("6");
// };

// const get_live_odds = async (hid) => {
//   let ob = await zed_db.db
//     .collection("odds_live")
//     .findOne({ hid: parseInt(hid) });
//   return ob;
// };

// const horse_tunnel = async (hid) => {
//   hid = parseInt(hid);
//   if (isNaN(hid)) return console.log("Invalid HID");
//   console.log("starting horse_tunnel for hid:", hid);
//   // let rid_1 = await get_first_raceid(hid);
//   // console.log("base_raceid: " rid_1);
//   let hs = await get_horses_in_race("eFdt7IqM");
//   console.table(hs);
// };
