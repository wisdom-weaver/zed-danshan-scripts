const _ = require("lodash");
const { zed_ch, zed_db } = require("../connection/mongo_connect");
const cyclic_depedency = require("../utils/cyclic_dependency");
const moment = require("moment");
const { iso, nano } = require("../utils/utils");

const run = async () => {
  let st = "2022-01-12T15:00:00Z";
  let ed = moment().toISOString();
  let now = new Date(st).getTime();
  let offset = 1000 * 60 * 60 * 1;

  let c = 3;
  let ds = [2200, 2400, 2600];
  let ob = {};
  while (now < new Date(ed).getTime()) {
    let now_ed = Math.min(now + offset, nano(ed));
    console.log(iso(now), "-->", iso(now_ed));
    let races =
      (await zed_ch.db
        .collection("zed")
        .find({ 2: { $gte: iso(now), $lte: iso(now_ed) } })
        .toArray()) || [];
    races = cyclic_depedency.struct_race_row_data(races);
    console.log("got tot", races.length);
    for (let row of races) {
      let { thisclass, distance, fee_tag, hid } = row;
      distance = parseFloat(distance);
      thisclass = parseFloat(thisclass);
      if (_.isEmpty(ob[hid])) ob[hid] = { tot_races_dur: 0, filt_races: 0 };
      ob[hid].tot_races_dur++;
      if (thisclass == c && ds.includes(distance) && fee_tag == "B")
        ob[hid].filt_races++;
    }
    now += offset;
  }
  let id = "test_run_01";
  await zed_db.db
    .collection("test")
    .updateOne({ id }, { $set: { id, ob } }, { upsert: true });
  console.log("wrote", _.keys(ob), "horses");
};

const tests = { run };
module.exports = tests;
