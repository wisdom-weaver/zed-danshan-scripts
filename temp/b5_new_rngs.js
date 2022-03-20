const cyclic_depedency = require("../code/utils/cyclic_dependency");
const { iso } = require("../code/utils/utils");
const v5_conf = require("../code/v5/v5_conf");
const v3rng = require("../code/v3/gaps");
const _ = require("lodash");
const sheet_ops = require("../sheet_ops/sheets_ops");
const { zed_db } = require("../code/connection/mongo_connect");

let range = "testing!A1";
let spreadsheetId = "1M4pC0dcTeqek6gj0mMSwAHoGt60fMgTh_wcWr72fdI8";

const get_new_born_rng = async (ob) => {
  try {
    let { hid, parents, tx_date } = ob;
    let { mother = null, father = null } = parents || {};
    let [father_rng, mother_rng, baby_rng] = await Promise.all(
      [father, mother, hid].map(v3rng.get)
    );
    return {
      hid,
      tx_date,
      father,
      mother,
      father_rng,
      mother_rng,
      baby_rng,
    };
  } catch (err) {
    return null;
  }
};

const fetch = async () => {
  let docs =
    (await zed_db.db
      .collection("horse_details")
      .find(
        { tx_date: { $gte: v5_conf.st_date } },
        { projection: { hid: 1, parents: 1, tx_date: 1 } }
      )
      .toArray()) || [];
  console.log("babies:", docs.length);
  // docs = docs.slice(0, 10);
  console.log("got all parents");
  let data = [];
  for (let chu of _.chunk(docs, 50)) {
    console.log(chu[0].hid, chu[chu.length - 1].hid);
    let mini = await Promise.all(chu.map(get_new_born_rng));
    mini = _.compact(mini);
    data.push(mini);
  }
  data = _.flatten(data);
  return data;
};

const update = (values) => {
  let conf = { range, spreadsheetId, values };
  return sheet_ops.push_to_sheet(conf);
};

const struct = (data) => {
  let keys = _.keys(data[0]);
  console.log(keys);
  let ar = _.map(data, _.values);
  let values = [keys, ...ar];
  return values;
};

const runner = async () => {
  let data = await fetch();
  if (_.isEmpty(data)) return console.log("no data found");
  let values = struct(data);
  let resp = await update(values);
  console.log(iso(), "completed", resp?.data);
};

// const cron_str = "* * * * *";
const cron_str = "*/20 * * * *";
const b5_new_rngs = {
  cron_str,
  runner,
};
module.exports = b5_new_rngs;
