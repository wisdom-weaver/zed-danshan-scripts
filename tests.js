const { generate_blood_mapping } = require("./index-odds-generator");
const { init } = require("./index-run");
const _ = require("lodash");
const mongoose = require("mongoose");
const { write_to_path } = require("./utils");
const app_root = require("app-root-path");

const test1 = async () => {
  await init();
  // let ob = [
  //   { a: null, b: 1 },
  //   { a: 2, b: 3 },
  //   { a: null, b: 2 },
  //   { a: null, b: 4 },
  // ];
  let zed_db = mongoose.connection;
  let datas = await zed_db.db.collection("leaderboard").find({}).toArray();
  for (let data of datas) {
    console.log("writing ", data.id, "...");
    write_to_path({
      file_path: `${app_root}/backup/leaderboard/${data.id}.json`,
      data,
    });
  }
};
module.exports = {
  test1,
};
