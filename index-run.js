const _ = require("lodash");
const { MongoClient } = require("mongodb");
const mongoose = require("mongoose");
const prompt = require("prompt-sync")();
const fetch = require("node-fetch");

require("dotenv").config();
const MONGO_ROOT_PASS = process.env.MONGO_ROOT_PASS;

const init = async () => {
  console.log("starting...");
  mongoose.connect(
    `mongodb+srv://root:${MONGO_ROOT_PASS}@cluster0.hvyg7.mongodb.net/zed`,
    { useNewUrlParser: true, useUnifiedTopology: true }
  );
  zed_db = mongoose.connection;
  zed_db.on(
    "error",
    console.error.bind(console, "!! Error connecting to MONGO")
  );
  zed_db.once("open", function (err, resp) {
    console.log("# MONGO connected");
  });
  return await zed_db;
};

const run_func = async (run_func) => {
  let zed_db = await init();
  await run_func();
  process.exit();
};

module.exports = {
  init,
  run_func,
};
