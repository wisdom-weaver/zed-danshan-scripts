const fs = require("fs");
const path = require("path");
const _ = require("lodash");
const { zed_ch, zed_db } = require("../connection/mongo_connect");
const cyclic_depedency = require("../utils/cyclic_dependency");
const moment = require("moment");
const { iso, nano, getv } = require("../utils/utils");
const mega = require("../v3/mega");
const utils = require("../utils/utils");
const races_scheduled = require("../races/races_scheduled");
const v3rng = require("../v3/gaps");
const v5_conf = require("../v5/v5_conf");
const sheet_ops = require("../../sheet_ops/sheets_ops");
const b5_new_rngs = require("../../temp/b5_new_rngs");
const { get_parents } = require("../utils/cyclic_dependency");
const {
  get_zed_raw_data,
  zed_races_zrapi_runner,
  zed_races_zrapi_rid_runner,
} = require("../races/races_base");
const send_weth = require("../payments/send_weth");

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
  let hid = 34750;
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
  console.table(ob);
};

const run_09 = async () => {
  const hids_all = utils.get_hids(1, 210000);
  let data = [];
  for (let chunk of _.chunk(hids_all, 2000)) {
    let ar =
      (await zed_db.db
        .collection("gap4")
        .find({ hid: { $in: chunk } })
        .toArray()) || {};
    let [a, b] = [ar[0].hid, ar[ar.length - 1].hid];
    console.log(`${a} -> ${b}`);
    data = [...data, ...ar];
  }
  console.table(data);
};

const run_10 = async () => {
  let ranges = [
    [0, 1],
    [1, 2],
    [2, 3],
    [3, 4],
    [4, 5],
    [5, 6],
    [6, 7],
    [7, 8],
    [8, 9],
  ];
  let data = [];
  for (let [mi, mx] of ranges) {
    let gap5docs = await zed_db.db
      .collection("gap5")
      .find({ gap: { $gte: mi, $lte: mx } })
      .limit(30)
      .toArray();
    let hids = _.map(gap5docs, "hid");
    gap5docs = _.keyBy(gap5docs, "hid");

    let gap6docs = await zed_db.db
      .collection("gap6")
      .find({ hid: { $in: hids } })
      .toArray();
    gap6docs = _.keyBy(gap6docs, "hid");
    let after_hids = _.keys(gap6docs);
    console.log(after_hids);

    data = [
      ...data,
      ...after_hids.map((hid) => {
        let { gap: before, date: before_date } = gap5docs[hid];
        let { gap: after, date: after_date } = gap6docs[hid];
        return { hid, before, after, before_date, after_date };
      }),
    ];
    console.log(mi, mx, "got", after_hids.length);
  }
  console.table(data);
};

const get_race_gap = (race) => {
  try {
    if (_.isEmpty(race)) return [];
    race = cyclic_depedency.struct_race_row_data(race);
    race = _.uniqBy(race, (e) => `${e.raceid}-${e.hid}`);
    // console.table(_.sortBy(race,i=>+i.place))
    race = race.map((ea) => {
      let { place, flame } = ea;
      place = parseInt(place);
      flame = parseInt(flame);
      return { ...ea, place, flame };
    });
    race = _.keyBy(race, "place");

    let raceid = race[1]?.raceid;
    let dist = race[1]?.distance;
    let date = race[1]?.date;
    dist = parseInt(dist);
    // console.log({ raceid, dist });

    let ar = [];
    if (race[1] && race[2]) {
      let gap_win = Math.abs(race[1].finishtime - race[2].finishtime);
      gap_win = (gap_win * 1000) / dist;
      let gap = gap_win;
      if (gap && date < "2021-08-30") gap = gap / 2;
      if ([0, 1].includes(race[1].flame))
        ar.push({
          hid: race[1].hid,
          gap,
          raceid,
          date,
          pos: race[1].place,
          flame: race[1].flame,
        });
    }
    if (race[1] && race[2]) {
      let gap_lose = Math.abs(race[11].finishtime - race[12].finishtime);
      gap_lose = (gap_lose * 1000) / dist;
      let gap = gap_lose;
      if (gap && date < "2021-08-30") gap = gap / 2;
      if ([1].includes(race[12].flame))
        ar.push({
          hid: race[12].hid,
          gap,
          raceid,
          date,
          pos: race[12].place,
          flame: race[12].flame,
        });
    }
    return ar;
  } catch (err) {
    let r = race && race[0];
    r = r && r[4];
    console.log(`err at ${r}`, err.message);
    return [];
  }
};
const run_raw_races = async (races_ar) => {
  races_ar = _.groupBy(races_ar, "4");
  console.log(`GAP:: got ${_.keys(races_ar).length} races`);
  for (let chunk of _.chunk(_.entries(races_ar), def_cs)) {
    let chunk_races = _.map(chunk, 1);
    await Promise.all(chunk_races.map(run_race));
  }
  // console.log("GAP:: DONE");
};

const run_11 = async () => {
  const hid = 9439;
  let rids = await zed_ch.db
    .collection("zed")
    .find({
      6: hid,
      // 8: { $in: [1, 2, 3, 10, 11, 12, "1", "2", "3", "10", "11", "12"] },
    })
    .toArray();
  rids = rids.map((e) => {
    return [e[4], e[8], e[13]];
  });
  console.log(rids);
  let ar = [];
  for (let [rid, horse_place, horse_flame] of rids) {
    let raw_race = await zed_ch.db.collection("zed").find({ 4: rid }).toArray();
    e = get_race_gap(raw_race);
    horse_place = parseFloat(horse_place);
    horse_flame = parseFloat(horse_flame);
    e = e.map((i) => {
      return { horse_place, horse_flame, ...i };
    });
    ar = [...ar, ...e];
  }
  console.table(ar);
};

const run_12 = races_scheduled.test;

const run_13 = async () => {
  const days = 10;
  // const days = 90;
  const ed = moment(new Date()).toISOString();
  const st = moment(new Date(utils.nano(ed)))
    // .subtract(days, "hour")
    .subtract(days, "days")
    .toISOString();
  console.log("st:", st);
  console.log("ed:", ed);
  let now = utils.nano(st);
  const offset = 60 * 60 * 1000;
  let ar = [];
  while (now < utils.nano(ed)) {
    const now_st = utils.iso(now);
    const now_ed = utils.iso(Math.min(utils.nano(ed), now + offset));
    let races = await zed_ch.db
      .collection("zed")
      .find(
        { 2: { $gte: now_st, $lte: now_ed } },
        { projection: { 1: 1, 7: 1 } }
      )
      .toArray();
    races = cyclic_depedency.struct_race_row_data(races);
    console.log(races.length, ":", now_st, now_ed);
    now = utils.nano(now_ed) + 1;
    ar.push(races);
  }
  ar = _.flatten(ar);
  // console.log(ar[0]);
  let ob = _.chain(ar)
    .groupBy("distance")
    .entries()
    .map(([d, e]) => [d, _.map(e, "finishtime")])
    .fromPairs()
    .value();
  for (let [d, tar = []] of _.entries(ob)) {
    d = parseFloat(d);
    tar = _.filter(tar, (i) => ![null, undefined, 0, NaN].includes(i));
    // console.log(tar);
    let time_min = _.min(tar);
    let time_avg = _.mean(tar);
    let time_median = utils.calc_median(tar);
    let time_max = _.max(tar);
    let speed_min = ((d / time_max) * 60 * 60) / 1000;
    let speed_avg = ((d / time_avg) * 60 * 60) / 1000;
    let speed_median = ((d / time_median) * 60 * 60) / 1000;
    let speed_max = ((d / time_min) * 60 * 60) / 1000;

    let n = tar.length;
    ob[d] = {
      n,
      time_min,
      time_avg,
      time_median,
      time_max,
      speed_min,
      speed_avg,
      speed_median,
      speed_max,
    };
  }
  console.table(ob);
};

const run_14 = async () => {
  const pr = [];
  let gts = [3, 2.5, 2, 1, 0.5];
  // let gts = [3];
  for (let gt of gts) {
    let hids = await zed_db.db
      .collection("gap4")
      .find({ gap: { $gte: gt } }, { projection: { _id: 0, hid: 1 } })
      .toArray();
    hids = _.compact(_.map(hids, "hid"));
    console.log(hids);
    let wins = await zed_db.db
      .collection("rating_blood3")
      .find(
        { hid: { $in: hids } },
        { projection: { _id: 1, "overall_rat.win_rate": 1 } }
      )
      .toArray();
    wins = _.filter(
      _.map(wins, "overall_rat.win_rate"),
      (i) => ![null, NaN, undefined, 0].includes(i)
    );
    let win_min = _.min(wins);
    let win_avg = _.mean(wins);
    let win_median = utils.calc_median(wins);
    let win_max = _.max(wins);
    pr.push({
      gap: `gap>=${utils.dec(gt)}`,
      n: wins.length,
      win_min,
      win_avg,
      win_median,
      win_max,
    });
  }
  console.table(pr);
};

const get_new_born_rng = async ([h, ps]) => {
  try {
    let hid = h;
    let { mother = null, father = null } = ps || {};
    let [father_rng, mother_rng, baby_rng] = await Promise.all(
      [father, mother, hid].map(v3rng.get)
    );
    return {
      hid,
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

const run_15 = async () => {
  let hids = await v5_conf.get_newborn_hids();
  console.log("babies:", hids.length);
  // hids = hids.slice(0, 10);
  let ar = await Promise.all(
    hids.map((h) => cyclic_depedency.get_parents(h).then((d) => [h, d]))
  );
  console.log("got all paarents");
  let data = [];
  for (let chu of _.chunk(ar, 20)) {
    console.log(chu[0][0], chu[chu.length - 1][0]);
    let mini = await Promise.all(chu.map(get_new_born_rng));
    mini = _.compact(mini);
    data.push(mini);
  }
  data = _.flatten(data);
  console.table(data);
};

const run_16 = async () => {
  let values = [[`=3+2`, 4, 5]];
  let range = "testing!B2:E2";
  let spreadsheetId = "1M4pC0dcTeqek6gj0mMSwAHoGt60fMgTh_wcWr72fdI8";
  let conf = { range, spreadsheetId, values };
  let ob1 = await sheet_ops.push_to_sheet(conf);
  console.log(ob1);
  console.log(ob1?.data?.updatedData);
};

const get_dp = async (hid) => {
  let dp4 = await zed_db.db
    .collection("dp4")
    .findOne({ hid }, { projection: { dp: 1, dist: 1 } });
  return dp4;
};

const getpda = async (baby) => {
  let parents = await get_parents(baby);
  let { mother, father } = parents;
  let [bdp, mdp, fdp] = await Promise.all([baby, mother, father].map(get_dp));
  let ob = {
    baby,
    baby_dp_dist: bdp?.dist,
    baby_dp_score: bdp?.dp,
    mother_dp_score: mdp?.dp,
    father_dp_score: fdp?.dp,
  };
  return ob;
};

const run_17 = async () => {
  const spreadsheetId = "1MWnILjDr71rW-Gp8HrKP6YnS03mJARygLSuS7xxsHhM";
  const range = "1616";
  let qu = { dist_f: 1600, dist_m: 1600 };
  let doc = await zed_db.db.collection("compiler_dp").findOne(qu);
  let hids = [1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600].map((d) =>
    getv(doc, `dist_ob.${d}.hids`)
  );
  hids = _.flatten(hids);
  // hids = hids.slice(0, 10);
  console.log("len: ", hids.length);
  let ar = [];
  let i = 0;
  for (let chu of _.chunk(hids, 10)) {
    let minar = await Promise.all(chu.map(getpda));
    ar.push(minar);
    i += minar.length;
    console.log(i);
  }
  ar = _.flatten(ar);
  let ob1 = await sheet_ops.sheet_print_ob(ar, { range, spreadsheetId });
  console.log(ob1.data);
};

const run_18 = async () => {
  let from = moment().subtract(6, "minutes").toISOString();
  let to = moment().subtract(5, "minutes").toISOString();
  // let ar = await get_zed_raw_data(from, to);
  let ar = await zed_races_zrapi_rid_runner("BfRJrovZ");
  console.log(ar);
};

const run_19 = async () => {
  let pays = [
    {
      WALLET: "0xCaD173Dc87DdfD5eD550030470c35d9BeC4BDE3d",
      AMOUNT: "0.6",
    },
  ];
  await send_weth.sendAllTransactions(
    pays,
    process.env.flash_payout_private_key
  );
};

const run_20 = async () => {
  let date = moment().subtract(1, "hour").toISOString();
  console.log(nano(date) / 1000);
};

const run_21 = async () => {
  let pays = [
    {
      WALLET: "0xc2014B17e2234ea18a50F292faEE29371126A3e0",
      AMOUNT: "0.3",
    },
  ];
  // await send_weth.sendAllTransactions(pays, process.env.flash_payout_private_key);
};

const run_22 = async () => {
  await zed_db.db
    .collection("users")
    .updateMany({}, { $set: { active: false } });
  console.log("done");
};

const run_23 = async () => {
  let ar = await zed_db.db
    .collection("horse_details")
    .aggregate([
      {
        $match: {
          hid: {
            $gte: 365000,
          },
          "parents.mother": {
            $ne: null,
          },
        },
      },
      {
        $project: {
          hid: 1,
          mother: "$parents.mother",
          father: "$parents.father",
        },
      },
      {
        $lookup: {
          from: "horse_details",
          localField: "mother",
          foreignField: "hid",
          as: "mdoc",
        },
      },
      {
        $lookup: {
          from: "horse_details",
          localField: "father",
          foreignField: "hid",
          as: "fdoc",
        },
      },
      {
        $lookup: {
          from: "dp4",
          localField: "mother",
          foreignField: "hid",
          as: "mdpdoc",
        },
      },
      {
        $lookup: {
          from: "dp4",
          localField: "father",
          foreignField: "hid",
          as: "fdpdoc",
        },
      },
      {
        $unwind: {
          path: "$mdoc",
          includeArrayIndex: "0",
          preserveNullAndEmptyArrays: false,
        },
      },
      {
        $unwind: {
          path: "$fdoc",
          includeArrayIndex: "0",
          preserveNullAndEmptyArrays: false,
        },
      },
      {
        $unwind: {
          path: "$mdpdoc",
          includeArrayIndex: "0",
          preserveNullAndEmptyArrays: false,
        },
      },
      {
        $unwind: {
          path: "$fdpdoc",
          includeArrayIndex: "0",
          preserveNullAndEmptyArrays: false,
        },
      },
      {
        $project: {
          hid: 1,
          mother: 1,
          father: 1,
          mother_bt: "$mdoc.breed_type",
          father_bt: "$fdoc.breed_type",
          mother_dist: "$mdpdoc.dist",
          father_dist: "$fdpdoc.dist",
        },
      },
      {
        $match: {
          mother_dist: { $ne: null },
          father_dist: { $ne: null },
        },
      },
      {
        $project: {
          hid: 1,
          mother: 1,
          father: 1,
          mother_bt: 1,
          father_bt: 1,
          mother_dist: 1,
          father_dist: 1,
          mother_tun: {
            $switch: {
              branches: [
                {
                  case: { $in: ["$mother_dist", [1000, 1200, 1400]] },
                  then: "SPRINTER",
                },
                {
                  case: { $in: ["$mother_dist", [1600, 1800, 2000]] },
                  then: "MID-RUN",
                },
                {
                  case: { $in: ["$mother_dist", [2200, 2400, 2600]] },
                  then: "MARATHON",
                },
              ],
              default: null,
            },
          },
          father_tun: {
            $switch: {
              branches: [
                {
                  case: { $in: ["$father_dist", [1000, 1200, 1400]] },
                  then: "SPRINTER",
                },
                {
                  case: { $in: ["$father_dist", [1600, 1800, 2000]] },
                  then: "MID-RUN",
                },
                {
                  case: { $in: ["$father_dist", [2200, 2400, 2600]] },
                  then: "MARATHON",
                },
              ],
              default: null,
            },
          },
        },
      },
      {
        $lookup: {
          from: "rcount",
          localField: "hid",
          foreignField: "hid",
          as: "rcountdoc",
        },
      },
      {
        $unwind: {
          path: "$rcountdoc",
          includeArrayIndex: "0",
          preserveNullAndEmptyArrays: false,
        },
      },
      {
        $project: {
          hid: 1,
          mother: 1,
          father: 1,
          mother_bt: 1,
          father_bt: 1,
          mother_dist: 1,
          father_dist: 1,
          mother_tun: 1,
          father_tun: 1,
          win_n: "$rcountdoc.dist_ob.99.9000.1.0",
          races_n: "$rcountdoc.dist_ob.99.9000.33.0",
        },
      },
      {
        $project: {
          hid: 1,
          mother: 1,
          father: 1,
          mother_bt: 1,
          father_bt: 1,
          win_n: 1,
          races_n: 1,
          mother_dist: 1,
          father_dist: 1,
          mother_tun: 1,
          father_tun: 1,
          win_rate: {
            $cond: [
              { $eq: ["$races_n", 0] },
              0,
              { $divide: ["$win_n", "$races_n"] },
            ],
          },
          same_tunn: {
            $cond: [{ $eq: ["$father_tun", "$mother_tun"] }, "SAME", "DIFF"],
          },
          key: {
            $cond: [
              { $eq: [{ $cmp: ["$father_bt", "$mother_bt"] }, -1] },
              { $concat: ["$mother_bt", "-", "$father_bt"] },
              { $concat: ["$father_bt", "-", "$mother_bt"] },
            ],
          },
        },
      },
      { $match: { races_n: { $ne: 0 } } },
      // { $limit: 15 },
      {
        $group: {
          _id: { $concat: ["$key", " -- [ ", "$same_tunn", " ]"] },
          // _id: { $concat: ["$key", " -- [ ", "$same_dist", " ]"] },
          avg_win_rate: { $avg: "$win_rate" },
          count: { $sum: 1 },
        },
      },
      { $sort: { _id: 1 } },
    ])
    .toArray();
  console.table(ar);
  if (true)
    await sheet_ops.sheet_print_ob(ar, {
      range: "same_tunnel",
      spreadsheetId: "1NjYzfWy-Ns52uMNXFbAYIPHRtDymrgX8TyKcM5h3RWU",
    });
};

const run_24 = async () => {
  let range_ar = [
    [1.4, 1e4],
    [1.3, 1.4],
    [1.2, 1.3],
    [1.1, 1.2],
    [1, 1.1],
    [0.7, 1],
    [0, 0.7],
  ];
  let ar = [];
  for (let [mi, mx] of range_ar) {
    let ea = await zed_db.db
      .collection("horse_details")
      .aggregate([
        {
          $match: {
            hid: {
              $gte: 365000,
            },
            "parents.mother": {
              $ne: null,
            },
          },
        },
        {
          $project: {
            hid: 1,
            mother: "$parents.mother",
            father: "$parents.father",
          },
        },
        {
          $lookup: {
            from: "horse_details",
            localField: "mother",
            foreignField: "hid",
            as: "mdoc",
          },
        },
        {
          $lookup: {
            from: "horse_details",
            localField: "father",
            foreignField: "hid",
            as: "fdoc",
          },
        },
        {
          $lookup: {
            from: "ymca5",
            localField: "mother",
            foreignField: "hid",
            as: "mdpdoc",
          },
        },
        {
          $lookup: {
            from: "ymca5",
            localField: "father",
            foreignField: "hid",
            as: "fdpdoc",
          },
        },
        {
          $unwind: {
            path: "$mdoc",
            includeArrayIndex: "0",
            preserveNullAndEmptyArrays: false,
          },
        },
        {
          $unwind: {
            path: "$fdoc",
            includeArrayIndex: "0",
            preserveNullAndEmptyArrays: false,
          },
        },
        {
          $unwind: {
            path: "$mdpdoc",
            includeArrayIndex: "0",
            preserveNullAndEmptyArrays: false,
          },
        },
        {
          $unwind: {
            path: "$fdpdoc",
            includeArrayIndex: "0",
            preserveNullAndEmptyArrays: false,
          },
        },
        {
          $project: {
            hid: 1,
            mother: 1,
            father: 1,
            mother_bt: "$mdoc.breed_type",
            father_bt: "$fdoc.breed_type",
            mother_ymca5: "$mdpdoc.ymca5",
            father_ymca5: "$fdpdoc.ymca5",
          },
        },
        {
          $match: {
            $and: [
              {
                mother_ymca5: { $gte: mi, $lt: mx },
                father_ymca5: { $gte: mi, $lt: mx },
              },
              {
                // $not: {
                $and: [
                  { mother_bt: { $eq: "genesis" } },
                  { father_bt: { $eq: "genesis" } },
                ],
                // },
              },
              // {
              //   father_ymca5: { $gte: 0.7, $lte: 1 },
              //   mother_ymca5: { $gte: 1.3 },
              // },
            ],
          },
        },
        {
          $lookup: {
            from: "rcount",
            localField: "hid",
            foreignField: "hid",
            as: "rcountdoc",
          },
        },
        {
          $unwind: {
            path: "$rcountdoc",
            includeArrayIndex: "0",
            preserveNullAndEmptyArrays: false,
          },
        },
        {
          $project: {
            hid: 1,
            mother: 1,
            father: 1,
            mother_bt: 1,
            father_bt: 1,
            mother_ymca5: 1,
            father_ymca5: 1,
            win_n: "$rcountdoc.dist_ob.99.9000.1.0",
            races_n: "$rcountdoc.dist_ob.99.9000.33.0",
          },
        },
        {
          $project: {
            hid: 1,
            mother: 1,
            father: 1,
            mother_bt: 1,
            father_bt: 1,
            win_n: 1,
            races_n: 1,
            mother_ymca5: 1,
            father_ymca5: 1,
            win_rate: {
              $cond: [
                { $eq: ["$races_n", 0] },
                0,
                { $divide: ["$win_n", "$races_n"] },
              ],
            },
          },
        },
        { $match: { races_n: { $ne: 0 } } },
        // { $limit: 15 },
        {
          $group: {
            _id: `${mi} - ${mx}`,
            // _id: { $concat: ["$key", " -- [ ", "$same_dist", " ]"] },
            avg_win_rate: { $avg: "$win_rate" },
            count: { $sum: 1 },
          },
        },
        { $sort: { _id: 1 } },
      ])
      .toArray();
    console.table(ea);
    ar.push(ea[0]);
  }
  console.table(ar);
  if (true)
    await sheet_ops.sheet_print_ob(ar, {
      range: "ymca_both_genesis",
      spreadsheetId: "1NjYzfWy-Ns52uMNXFbAYIPHRtDymrgX8TyKcM5h3RWU",
    });
};

const tests = { run: run_21 };
module.exports = tests;
