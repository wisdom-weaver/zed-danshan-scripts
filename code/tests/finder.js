const _ = require("lodash");
const { fget } = require("../utils/fetch");
const zedf = require("../utils/zedf");

const get_owner_of = async (hid) => {
  let doc = await zedf.horse(hid);
  return doc?.owner;
};

let racing = `https://racing-api.zed.run/api/v1/races`;
const get_races = async (status) => {
  let ar = await zedf.get(`${racing}?status=${status}`);
  for (let r of ar) {
    console.log("--\n\n");
    let {
      race_id,
      status: race_status,
      start_time,
      class: race_class,
      length: dist,
      gates,
      final_positions,
    } = r;
    console.log(race_status, "race", race_id, start_time);
    let hids = [];
    if (race_status == "finished")
      hids = _.chain(final_positions).values().map("horse_id").value();
    if (race_status == "scheduled") hids = _.chain(gates).values().value();
    if (race_status == "open")
      hids = _.chain(gates).values().map("horse_id").value();
    let own_ob = await Promise.all(
      hids.map((hid) => get_owner_of(hid).then((d) => [hid, d]))
    );
    own_ob = _.fromPairs(own_ob);
    hids.map((hid) => console.log(hid, own_ob[hid]));
  }
};

const get_open = async () => get_races("open");
const get_scheduled = async () => get_races("scheduled");

const test = async () => {
  let doc = await zedf.get(
    "https://racing-api.zed.run/api/v1/races?status=live"
  );
  console.log(doc);
};

const finder = {
  get_races,
  get_open,
  get_scheduled,
  test,
};
module.exports = finder;
