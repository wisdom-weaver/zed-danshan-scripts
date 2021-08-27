const _ = require("lodash");
const { MongoClient } = require("mongodb");
const mongoose = require("mongoose");
const prompt = require("prompt-sync")();
const fetch = require("node-fetch");

require("dotenv").config();
const MONGO_ROOT_PASS = process.env.MONGO_ROOT_PASS;

const uri_db = `mongodb+srv://root:${MONGO_ROOT_PASS}@cluster0.hvyg7.mongodb.net`;
const uri_ch = `mongodb+srv://zed:zed@cluster0.vyaud.mongodb.net`;
const options = { useNewUrlParser: true, useUnifiedTopology: true };

// let conn_db = MongoClient.connect(uri_db, options);
// let conn_ch = MongoClient.connect(uri_ch, options);

let zed_db = mongoose.createConnection(uri_db + "/zed", options);
let zed_ch = mongoose.createConnection(uri_ch + "/zed", options);

const init = async () => {
  console.log("starting...");
  await zed_db;
  console.log("# MONGO connected zed_db");
  await zed_ch;
  console.log("# MONGO connected zed_ch");
};

const run_func = async (run_func) => {
  let zed_db = await init();
  await run_func();
  process.exit();
};

module.exports = {
  init,
  run_func,
  zed_db,
  zed_ch,
};
