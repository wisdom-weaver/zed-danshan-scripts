const _ = require("lodash");
const { zed_db } = require("./index-run");
const { fetch_r } = require("./utils");

const horse_exists = async (hid) => {
  hid = parseInt(hid);
  let api = `https://api.zed.run/api/v1/horses/get/${hid}`;
  let doc = await fetch_r(api);
  if (_.isEmpty(doc)) return undefined;
  if (doc.error) return false;
  return true;
};

const get_max_horse = async () => {
  let ob = await zed_db.collection("base").findOne({ id: "mx_hid" });
  return ob?.mx_hid || 88000;
};

const generate_max_horse = async () => {
  console.log("finding max hid right now");
  let prev_exists = await get_max_horse();
  let [st, ed] = [prev_exists, prev_exists + 3000];
  while (st <= ed) {
    let mid = parseInt((st + ed) / 2);
    let mid_exists = await horse_exists(mid);
    if (mid_exists === undefined) continue;
    if (mid_exists === true) {
      prev_exists = Math.max(prev_exists, mid);
      st = mid + 1;
    } else {
      ed = mid - 1;
    }
  }
  let db_date = new Date().toISOString();
  let ob = { mx_hid: prev_exists, db_date };
  await zed_db
    .collection("base")
    .updateOne({ id: "mx_hid" }, { $set: ob }, { upsert: true });
  console.log("got max_horse:", ob?.mx_hid);
  return prev_exists;
};

module.exports = {
  generate_max_horse,
  get_max_horse,
};
