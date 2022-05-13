const _ = require("lodash");
const { MongoClient } = require("mongodb");
const mongoose = require("mongoose");
const fetch = require("node-fetch");

require("dotenv").config();
const MONGO_ROOT_PASS = process.env.MONGO_ROOT_PASS;

const uri_db = process.env.MONGO_DB_URI;
const uri_ch = process.env.MONGO_CH_URI;
const options = { useNewUrlParser: true, useUnifiedTopology: true };

// let conn_db = MongoClient.connect(uri_db, options);
// let conn_ch = MongoClient.connect(uri_ch, options);

let zed_db = mongoose.createConnection(uri_db + "/zed", options);
let zed_ch = mongoose.createConnection(uri_ch + "/zed", options);

const init = async () => {
  console.log("starting...");
  try {
    await zed_db;
    console.log("# MONGO connected zed_db");
  } catch (err) { console.log("zed_db", err.message) }
  try {
    await zed_ch;
    console.log("# MONGO connected zed_ch");
  } catch (err) { console.log("zed_ch err", err.message) }
};

const run_func = async (run_func) => {
  await init();
  await run_func();
  process.exit();
};

module.exports = {
  init,
  run_func,
  zed_db,
  zed_ch,
};
