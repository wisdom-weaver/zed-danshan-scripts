const fs = require("fs");
const path = require("path");
const _ = require("lodash");
const { zed_ch, zed_db } = require("../connection/mongo_connect");
const cyclic_depedency = require("../utils/cyclic_dependency");
const moment = require("moment");
const { iso, nano } = require("../utils/utils");
const mega = require("../v3/mega");
const utils = require("../utils/utils");

const run_01 = async () => {
  let st = "2022-01-06T00:00:00Z";
  // let st = moment().subtract(3, "hours").toISOString();
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

const run_04 = async () => {
  let hid_mx = 185000;
  await zed_db.db
    .collection("horse_details")
    .deleteMany({ hid: { $gte: hid_mx } });
};

const run_05 = async (range) => {
  console.log("tests run05");
  if (!range) return console.log("end");
  let [st, ed] = range;
  st = utils.get_n(st);
  ed = utils.get_n(ed);
  console.log([st, ed]);
  let hids = await zed_db.db
    .collection("rating_breed3")
    .find({ br: null, hid: { $lte: ed, $gte: st } }, { projection: { hid: 1 } })
    .toArray();
  console.log(hids);
  hids = _.map(hids, "hid");
  await mega.only(hids);
};

const run_06 = async () => {
  console.log("run_06");
  let docs = [];
  let st = nano("2022-01-01T00:00Z");
  let ed = Date.now(); // nano("2022-01-02T00:00Z") //
  let now = st;
  let off = 1 * 60 * 60 * 1000;
  while (now < ed) {
    console.log([iso(now), iso(now + off)]);
    let ndocs = await zed_ch.db
      .collection("zed")
      .find(
        {
          2: {
            $gte: iso(now),
            $lte: iso(now + off),
          },
        },
        {
          projection: {
            // 1:1, //"distance",
            2: 1, //"date",
            3: 1, //"entryfee",
            4: 1, //"raceid",
            5: 1, //"thisclass",
            6: 1, //"hid",
            // 7:1, //"finishtime",
            // 8:1, //"place",
            // 9:1, //"name",
            // 10:1, //"gate",
            // 11:1, //"odds",
            // 12:1, //"unknown",
            13: 1, //"flame",
            // 14:1, //"fee_cat",
            // 15:1, //"adjfinishtime",
            // 16:1, //"htc",
            // 17:1, //"race_name",
          },
        }
      )
      .toArray();
    docs = [...docs, ...(ndocs || [])];
    console.log("got", ndocs.length);
    now += off;
  }
  let ob = {};
  ob = _.groupBy(docs, "5");
  ob = _.entries(ob).map(([c, rs]) => {
    let filt = _.filter(rs, (r) => r[5] == 99 || parseFloat(r[3]) != 0);
    let filt_f = _.filter(filt, { 13: 1 });
    let races_n = _.uniqBy(filt, (i) => i[4])?.length;
    let uniq_hids = _.uniq(_.map(filt, 6));
    console;
    let uniq_hids_f = _.uniq(_.map(filt_f, 6));
    let new_horses = uniq_hids.filter((e) => e >= 178250);
    let new_horses_f = uniq_hids_f.filter((e) => e >= 178250);
    return {
      class: c,
      races_n,
      uniq_hids: uniq_hids.length,
      uniq_hids_f: uniq_hids_f.length,
      new_horses: new_horses.length,
      new_horses_f: new_horses_f.length,
    };
  });
  console.table(ob);
};

const run_07 = async () => {
  let st = nano("2022-01-01T00:00Z");
  let ed = Date.now(); // nano("2022-01-02T00:00Z") //
  let ob = {};
  for (let c of [1, 2, 3, 4, 5, 99]) {
    let ndocs = await zed_ch.db
      .collection("zed")
      .find(
        {
          2: {
            $gte: iso(st),
            $lte: iso(ed),
          },
          5: c,
          // 6: { $gte: 178250 },
          8: { $in: [1, "1"] },
          // 13: 1,
          ...(c !== 99 ? { 3: { $ne: "0.0" } } : {}),
        },
        {
          projection: {
            // 1:1, //"distance",
            2: 1, //"date",
            3: 1, //"entryfee",
            4: 1, //"raceid",
            // 5: 1, //"thisclass",
            6: 1, //"hid",
            // 7:1, //"finishtime",
            // 8: 1, //"place",
            // 9:1, //"name",
            // 10:1, //"gate",
            // 11:1, //"odds",
            // 12:1, //"unknown",
            13: 1, //"flame",
            // 14:1, //"fee_cat",
            // 15:1, //"adjfinishtime",
            // 16:1, //"htc",
            // 17:1, //"race_name",
          },
        }
      )
      .toArray();
    ndocs = cyclic_depedency.struct_race_row_data(ndocs);
    if (c !== 99)
      ndocs = ndocs.filter((e) => ["A", "B", "C", "D"].includes(e.fee_tag));
    // console.table(ndocs);
    let hids = _.uniq(_.map(ndocs, "hid"));
    let new_hids = _.filter(hids, (e) => e > 178250);

    let ndocs_F = _.filter(ndocs, { flame: 1 });
    let hids_F = _.uniq(_.map(ndocs_F, "hid"));
    let new_hids_F = _.filter(hids_F, (e) => e > 178250);

    races_n = _.uniqBy(ndocs, (i) => i.raceid)?.length;

    hids = _.uniq(hids);
    ob[c] = hids;
    console.log("Class", c);
    console.log("races_n", races_n);
    console.log(hids.length, "hids\n", hids);
    console.log(hids_F.length, "hids_F\n", hids_F);
    console.log(new_hids.length, "new_hids\n", new_hids);
    console.log(new_hids_F.length, "new_hids_F\n", new_hids_F);
    console.log("===========================");
    ob[c] = {
      races_n,
      hids: hids.length,
      hids_F: hids_F.length,
      new_hids: new_hids.length,
      new_hids_F: new_hids_F.length,
    };
  }
  console.table(ob);
};

const run_08 = async () => {
  let hid = 34750
  // let hid = 19526
  let ob = {};
  for (let d of [1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600]) {
    ob[d] = {};
    for (let p of [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]) {
      ob[d][p] = await zed_ch.db.collection("zed").countDocuments({
        6: hid,
        1: { $in: [d, d.toString()] },
        8: { $in: [p, p.toString()] },
      });
    }
  }
  console.table(ob)
};

const tests = { run: run_08 };
module.exports = tests;
