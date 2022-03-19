const _ = require("lodash");
const { zed_db } = require("../connection/mongo_connect");
const st_date = "2022-03-17T15:00:00Z";
// {tx_date:{$gte:"2022-03-17T15:00:00Z"}}
const v_code = 5;

const get_newborn_hids = async () => {
  let docs =
    (await zed_db.db
      .collection("horse_details")
      .find({ tx_date: { $gte: st_date } }, { projection: { hid: 1 } })
      .toArray()) || [];
  let hids = _.map(docs, "hid");
  return hids;
};

const v5_conf = { st_date, get_newborn_hids };
module.exports = v5_conf;
