const _ = require("lodash");
const { zed_db } = require("../connection/mongo_connect");
const ymca2_table = require("../v3/ymca2_table");
const ymca5_table = require("../v5/ymca5_table");

let data = {
  z_ALL: null,
  ymca2_avgs: null,
  ymca5_avgs: null,
};

const get_z_ALL_meds = async () => {
  let doc = await zed_db.db.collection("z_meds").findOne({ id: "z_ALL" });
  let ob = _.chain(doc.ar).keyBy("id").mapValues("med").value();
  console.log("GOT", "z_ALL");
  return ob;
};

const download = async () => {
  data.ymca2_avgs = await ymca2_table.get();
  data.ymca5_avgs = await ymca5_table.get();
  data.z_ALL = await get_z_ALL_meds();
};

const get_data = () => data;

const global_req = { data, download, get_data };

module.exports = global_req;
