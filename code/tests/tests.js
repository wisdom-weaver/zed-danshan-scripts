const fs = require("fs");
const path = require("path");
const _ = require("lodash");
const { zed_ch, zed_db } = require("../connection/mongo_connect");
const cyclic_depedency = require("../utils/cyclic_dependency");
const moment = require("moment");
const { iso, nano } = require("../utils/utils");

const run_01 = async () => {
  // let st = "2022-01-06T00:00:00Z";
  let st = moment().subtract(3, "hours").toISOString();
  let ed = moment().toISOString();
  let now = new Date(st).getTime();
  let offset = 1000 * 60 * 60 * 1;

  let c = 3;
  let ds = [2200, 2400, 2600];
  let races_n = 0;
  let rids = [];
  let hid_ob = {};
  while (now < new Date(ed).getTime()) {
    let now_ed = Math.min(now + offset, nano(ed));
    console.log(iso(now), "-->", iso(now_ed));
    let races =
      (await zed_ch.db
        .collection("zed")
        .find({
          2: { $gte: iso(now), $lte: iso(now_ed) },
          5: c,
          1: { $in: ds },
        }) //.limit(10)
        .toArray()) || [];
    races = cyclic_depedency.struct_race_row_data(races);
    races = _.groupBy(races, "raceid");
    console.log("got tot", _.keys(races).length);
    for (let [rid, ar] of _.entries(races)) {
      if (_.isEmpty(ar)) continue;
      let { thisclass, distance, fee_tag } = ar[0];
      thisclass = parseFloat(thisclass);
      distance = parseFloat(distance);
      if (thisclass == c && ds.includes(distance) && fee_tag == "B") {
        console.log("found", rid);
        rids.push(rid);
        races_n++;
        let hids = _.map(ar, "hid");
        hids.map((hid) => {
          if (hid_ob[hid] == undefined) hid_ob[hid] = 0;
          hid_ob[hid]++;
        });
      }
    }
    now += offset;
  }
  let final_ob = _.entries(hid_ob).map(([hid, count]) => {
    let per = count / races_n;
    return { hid, count, per };
  });

  let id = "test_run_01";
  let ob = {
    id,
    races_n,
    rids,
    hid_ob,
    final_ob,
  };
  await zed_db.db
    .collection("test")
    .updateOne({ id }, { $set: ob }, { upsert: true });
  console.table(final_ob);
  console.log("wrote", _.keys(hid_ob).length, "horses in", races_n);
};

const run_02 = async () => {
  let id = "test_run_01";
  let doc = await zed_db.db.collection("test").findOne({ id });
  console.table(doc.final_ob);
  console.table(doc.races_n);
};

const run_03 = async () => {
  let f = path.resolve(__dirname, "./test.txt");
  let txt = fs.readFileSync(f, { encoding: "utf-8" });
  let ar = [];
  for (let row of txt.split("\n")) {
    let [hid, tot, filt] = row.split("\t");
    hid = parseFloat(hid);
    filt = parseFloat(filt);
    tot = parseFloat(tot);
    console.log({ hid, filt, tot });
    ar.push({ hid, filt, tot });
  }
  ar = ar.filter((i) => i.filt > 0);
  console.table(ar);
};

const tests = { run: run_01 };
module.exports = tests;
