const fs = require("fs");
const path = require("path");
const _ = require("lodash");
const { zed_ch, zed_db } = require("../connection/mongo_connect");
const cyclic_depedency = require("../utils/cyclic_dependency");
const moment = require("moment");
const {
  iso,
  nano,
  getv,
  write_to_path,
  read_from_path,
  calc_median,
  cdelay,
} = require("../utils/utils");
const mega = require("../v3/mega");
const utils = require("../utils/utils");
const races_scheduled = require("../races/races_scheduled");
const v3rng = require("../v3/gaps");
const v5_conf = require("../v5/v5_conf");
const sheet_ops = require("../../sheet_ops/sheets_ops");
const b5_new_rngs = require("../../temp/b5_new_rngs");
const {
  get_parents,
  get_races_of_hid,
  get_ed_horse,
  get_range_hids,
  ag_look,
  get_date_range_fromto,
  struct_race_row_data,
  jstr,
} = require("../utils/cyclic_dependency");
const {
  get_zed_raw_data,
  zed_races_zrapi_runner,
  zed_races_zrapi_rid_runner,
} = require("../races/races_base");
const send_weth = require("../payments/send_weth");
const { dist_factor } = require("../v5/speed");
const { knex_conn } = require("../connection/knex_connect");
const { preset_global } = require("../races/sims");
const { rfn } = require("../connection/redis");
const red = require("../connection/redis");
const { sheet_print_ob } = require("../../sheet_ops/sheets_ops");

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
      WALLET: "0x33F0F57DCd106DF64FA2B8991cd6bDAe8f53dcf5",
      AMOUNT: "0.1064",
    },
  ];
  console.table(pays);
  await cdelay(5000);
  // await send_weth.sendAllTransactions(
  //   pays,
  //   process.env.flash_payout_private_key
  // );
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

const run_25 = async () => {
  let rg = [
    [97, 97],
    [96, 95],
    [95, 95],
    [94, 94],
    [95, 93],
    [93, 95],
  ];
  let ar = [];
  for (let [msp, fsp] of rg) {
    let ea = await zed_db.db
      .collection("horse_details")
      .aggregate([
        {
          $match: {
            hid: { $gte: 360000 },
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
          $match: {
            mother: {
              $ne: null,
            },
            father: {
              $ne: null,
            },
          },
        },
        {
          $lookup: {
            from: "speed",
            localField: "mother",
            foreignField: "hid",
            as: "sp_doc_m",
          },
        },
        {
          $unwind: {
            path: "$sp_doc_m",
            includeArrayIndex: "0",
            preserveNullAndEmptyArrays: false,
          },
        },
        {
          $lookup: {
            from: "speed",
            localField: "father",
            foreignField: "hid",
            as: "sp_doc_f",
          },
        },
        {
          $unwind: {
            path: "$sp_doc_f",
            includeArrayIndex: "0",
            preserveNullAndEmptyArrays: false,
          },
        },
        {
          $project: {
            hid: 1,
            mother: 1,
            mspeed: {
              $ifNull: ["$sp_doc_m.speed", null],
            },
            father: 1,
            fspeed: {
              $ifNull: ["$sp_doc_f.speed", null],
            },
          },
        },
        {
          $match: {
            mspeed: {
              $gte: msp,
            },
            fspeed: {
              $gte: fsp,
            },
          },
        },
        // { $limit: 10 },
        {
          $lookup: {
            from: "speed",
            localField: "hid",
            foreignField: "hid",
            as: "sp_doc",
          },
        },
        {
          $unwind: {
            path: "$sp_doc",
            includeArrayIndex: "0",
            preserveNullAndEmptyArrays: true,
          },
        },
        {
          $project: {
            hid: 1,
            mother: 1,
            mspeed: 1,
            father: 1,
            fspeed: 1,
            speed: {
              $ifNull: ["$sp_doc.speed", null],
            },
          },
        },
        {
          $group: {
            _id: `${msp} ${fsp}`,
            count_all: {
              $sum: 1,
            },
            count_valid_speed: {
              $sum: {
                $cond: [{ $ne: ["$speed", null] }, 1, 0],
              },
            },
            avg_baby_speed: {
              $avg: "$speed",
            },
            babies: {
              $push: "$hid",
            },
          },
        },
      ])
      .toArray();
    if (_.isEmpty(ea)) continue;
    let ob = ea[0];
    let doc = {
      _id: ob._id,
      msp,
      fsp,
      count_all: ob.count_all,
      count_valid_speed: ob.count_valid_speed,
      avg_baby_speed: ob.avg_baby_speed,
      babies: ob.babies?.join(", "),
    };
    ar.push(doc);
    console.log(doc);
  }
  console.table(ar);
  await sheet_ops.sheet_print_ob(ar, {
    range: "Speedbreed!A9",
    spreadsheetId: "1Coj3voJ6XiOMgdBO3M91DoDWrsSObPAxwOA5luBRHo0",
  });
};
const run_26 = async () => {
  console.log("run 26");
  let ar = await zed_db.db
    .collection("horse_details")
    .aggregate([
      // {
      //   $match: {
      //     hid: {
      //       $gte: 360000,
      //     },
      //   },
      // },
      {
        $lookup: {
          from: "speed",
          localField: "hid",
          foreignField: "hid",
          as: "sp_doc",
        },
      },
      {
        $unwind: {
          path: "$sp_doc",
          includeArrayIndex: "0",
          preserveNullAndEmptyArrays: false,
        },
      },
      {
        $project: {
          _id: 0,

          hid: 1,
          speed: "$sp_doc.speed",
        },
      },
      {
        $sort: {
          speed: -1,
        },
      },
      {
        $limit: 10,
      },
    ])
    .toArray();
  ar = ar.map((e, i) => ({ rank: i + 1, ...e }));
  console.table(ar);

  await sheet_ops.sheet_print_ob(ar, {
    range: "Speedbreed!E20",
    spreadsheetId: "1Coj3voJ6XiOMgdBO3M91DoDWrsSObPAxwOA5luBRHo0",
  });
};

const run_27 = async () => {
  let dist = 1000;
  let row = 16;
  let lim = 3;
  let rar = [
    [329925, `F${row}`, `F${row + 1}`],
    [342190, `G${row}`, `G${row + 1}`],
    [7700, `H${row}`, `H${row + 1}`],
  ];
  for (let [hid, cell0, cell1] of rar) {
    let races = await get_races_of_hid(hid);
    let ar = _.chain(races)
      .filter((e) => parseInt(e.distance) == dist)
      .sortBy((e) => e.finishtime)
      .map((e) => {
        let { finishtime, raceid, distance } = e;
        return { finishtime };
      })
      .slice(0, lim)
      .value();
    let avg = _.meanBy(ar, "finishtime");
    let speed_init = ((distance / finishtime) * 60 * 60) / 1000;
    let speed = dist_factor[distance] * speed_init;
    console.log({ hid, speed, speed_init });
    if (process.argv.includes("write")) {
      await sheet_ops.sheet_print_cell(avg, {
        range: `Sheet7!${cell0}`,
        spreadsheetId: "1Coj3voJ6XiOMgdBO3M91DoDWrsSObPAxwOA5luBRHo0",
      });
      await sheet_ops.sheet_print_cell(avg, {
        range: `Sheet7!${cell0}`,
        spreadsheetId: "1Coj3voJ6XiOMgdBO3M91DoDWrsSObPAxwOA5luBRHo0",
      });
    }
  }
};

const run_28 = async () => {
  let finishtime = parseFloat(process.argv[4]);
  // let cell0 = process.argv[5];
  let distance = parseFloat(process.argv[5]);
  let cell1 = process.argv[6];
  let speed_init = ((distance / finishtime) * 60 * 60) / 1000;
  let speed = dist_factor[distance] * speed_init;
  speed = speed * 1.45;
  console.log({ speed_init, speed });
  // await sheet_ops.sheet_print_cell(speed_init, {
  //   range: `Sheet7!${cell0}`,
  //   spreadsheetId: "1Coj3voJ6XiOMgdBO3M91DoDWrsSObPAxwOA5luBRHo0",
  // });
  await sheet_ops.sheet_print_cell(speed, {
    range: `FastestTimeLogic!${cell1}`,
    spreadsheetId: "1Coj3voJ6XiOMgdBO3M91DoDWrsSObPAxwOA5luBRHo0",
  });
};

const run_29 = async () => {
  // let dist = 1600;
  let hid = 128497;
  // let cell0 = "C4";
  let aaa = [
    [1600, "A4"],
    [1800, "B4"],
    [2000, "C4"],
  ];
  let races = await get_races_of_hid(hid);
  for (let [distance, cell] of aaa) {
    let ar = _.chain(races)
      .filter((e) => parseInt(e.distance) == distance)
      .sortBy((e) => e.finishtime)
      .map((e) => {
        let { finishtime, raceid, distance } = e;
        return { [distance]: finishtime };
      })
      // .slice(0, lim)
      .value();
    console.table(ar);
    if (process.argv.includes("write")) {
      await sheet_ops.sheet_print_ob(ar, {
        range: `FastestTimeLogic!${cell}`,
        spreadsheetId: "1Coj3voJ6XiOMgdBO3M91DoDWrsSObPAxwOA5luBRHo0",
      });
    }
  }
};

const run_30 = async () => {
  let ar = [];
  let cs = 1000;
  let ed = 500000; //await get_ed_horse();
  for (let i = 0; i <= ed; i += cs) {
    console.log(i, i + cs);
    let hids = await get_range_hids(i, i + cs);
    let ea = await zed_db.db
      .collection("speed")
      .find({ hid: { $in: hids } })
      .toArray();
    ar.push(ea);
  }
  ar = _.flatten(ar);
  write_to_path({ file_path: "speeds_data.json", data: ar });
  console.log("done");
};

const run_31 = async () => {
  let ar = read_from_path({ file_path: "speeds_data.json" });
  ar = _.filter(ar, (e) => e.speed != null);
  ar = _.sortBy(ar, (e) => -parseFloat(e.speed));
  let ob = [];
  for (let per of [1, 2, 5, 10, 20, 25, 30, 40]) {
    let n = ar.length;
    let count = parseInt((ar.length * per) / 100);
    let eaar = _.slice(ar, 0, count);
    let hids = _.map(ar, "hid").slice(0, 10).join(", ");
    let min = _.minBy(eaar, (e) => e.speed)?.speed;
    let max = _.maxBy(eaar, (e) => e.speed)?.speed;
    let avg = _.meanBy(eaar, (e) => e.speed);
    let ea = {
      total: n,
      top: per,
      count_top: count,
      avg,
      min,
      max,
      hids,
    };
    ob.push(ea);
  }
  console.table(ob);
};

const run_32 = async () => {
  let ar = await zed_db.db
    .collection("horse_details")
    .aggregate([
      { $match: { hid: { $gte: 360000 } } },
      {
        $project: {
          hid: 1,
          father: "$parents.father",
          mother: "$parents.mother",
        },
      },
      {
        $match: {
          mother: { $ne: null },
          father: { $ne: null },
        },
      },
      {
        $lookup: {
          from: "speed",
          localField: "mother",
          foreignField: "hid",
          as: "spdoc_m",
        },
      },
      {
        $unwind: {
          path: "$spdoc_m",
          includeArrayIndex: "0",
          preserveNullAndEmptyArrays: false,
        },
      },
      {
        $lookup: {
          from: "speed",
          localField: "father",
          foreignField: "hid",
          as: "spdoc_f",
        },
      },
      {
        $unwind: {
          path: "$spdoc_f",
          includeArrayIndex: "0",
          preserveNullAndEmptyArrays: false,
        },
      },
      {
        $project: {
          hid: 1,
          mother: 1,
          father: 1,
          speed_m: "$spdoc_m.speed",
          speed_f: "$spdoc_f.speed",
        },
      },
      {
        $lookup: {
          from: "speed",
          localField: "hid",
          foreignField: "hid",
          as: "spdoc",
        },
      },
      {
        $unwind: {
          path: "$spdoc",
          includeArrayIndex: "0",
          preserveNullAndEmptyArrays: false,
        },
      },
      {
        $project: {
          hid: 1,
          speed: "$spdoc.speed",
          mother: 1,
          speed_m: 1,
          father: 1,
          speed_f: 1,
        },
      },
      {
        $match: {
          $expr: {
            $and: [
              { $ne: ["$speed", "null"] },
              { $gte: ["$speed", 94] },
              { $gt: ["$speed", "$speed_f"] },
              { $gt: ["$speed", "$speed_m"] },
            ],
          },
        },
      },
      { $sort: { speed: -1 } },
      // {
      //   $match: {
      //     $or: [
      //       { speed_m: { $gte: range[0][0], $lte: range[0][1] } },
      //       { speed_f: { $gte: range[1][0], $lte: range[1][1] } },
      //     ],
      //   },
      // },
      // { $limit: 30 },
    ])
    .toArray();
  // console.table(ar);
  // if (process.argv.includes("write")) {
  if (true) {
    await sheet_ops.sheet_print_ob(ar, {
      range: `sp94!$A1`,
      spreadsheetId: "1Coj3voJ6XiOMgdBO3M91DoDWrsSObPAxwOA5luBRHo0",
    });
  }
};

const run_33 = async () => {
  let hids = [24393, 147719];
  let range = [];
  for (let hid of hids) {
    let doc = await zed_db.db.collection("speed").findOne({ hid });
    let val = getv(doc, "br");
    let [spmi, spmx] = [val * 0.9995, val * 1.0005];
    console.log(spmi, spmx);
    range.push([spmi, spmx]);
  }
  console.log(range);
  let ar = await zed_db.db
    .collection("horse_details")
    .aggregate([
      {
        $project: {
          hid: 1,
          father: "$parents.father",
          mother: "$parents.mother",
        },
      },
      {
        $match: {
          mother: {
            $ne: null,
          },
          father: {
            $ne: null,
          },
        },
      },
      // {
      //   $lookup: {
      //     from: "speed",
      //     localField: "mother",
      //     foreignField: "hid",
      //     as: "spdoc_m",
      //   },
      // },
      // {
      //   $unwind: {
      //     path: "$spdoc_m",
      //     includeArrayIndex: "0",
      //     preserveNullAndEmptyArrays: false,
      //   },
      // },
      // {
      //   $lookup: {
      //     from: "speed",
      //     localField: "father",
      //     foreignField: "hid",
      //     as: "spdoc_f",
      //   },
      // },
      // {
      //   $unwind: {
      //     path: "$spdoc_f",
      //     includeArrayIndex: "0",
      //     preserveNullAndEmptyArrays: false,
      //   },
      // },
      ...ag_look("rating_breed5", "mother", "hid", "brdoc_m", false),
      ...ag_look("rating_breed5", "father", "hid", "brdoc_f", false),
      {
        $project: {
          hid: 1,
          mother: 1,
          father: 1,
          // speed_m: "$spdoc_m.speed",
          // speed_f: "$spdoc_f.speed",
          br_m: "$brdoc_m.speed",
          br_f: "$brdoc_f.speed",
        },
      },
      {
        $match: {
          $or: [
            {
              $and: [
                { ["brdoc_m"]: { $gte: range[0][0], $lte: range[0][1] } },
                { ["brdoc_f"]: { $gte: range[1][0], $lte: range[1][1] } },
              ],
            },
            {
              $and: [
                { ["brdoc_f"]: { $gte: range[0][0], $lte: range[0][1] } },
                { ["brdoc_m"]: { $gte: range[1][0], $lte: range[1][1] } },
              ],
            },
          ],
        },
      },
      {
        $lookup: {
          from: "speed",
          localField: "hid",
          foreignField: "hid",
          as: "spdoc",
        },
      },
      {
        $unwind: {
          path: "$spdoc",
          includeArrayIndex: "0",
          preserveNullAndEmptyArrays: false,
        },
      },
      {
        $project: {
          hid: 1,
          speed: "$spdoc.speed",
          mother: 1,
          speed_m: 1,
          father: 1,
          speed_f: 1,
        },
      },
    ])
    .toArray();
  console.log(ar);

  if (process.argv.includes("write")) {
    await sheet_ops.sheet_print_ob(ar, {
      range: `Analyzer 6!$A30`,
      spreadsheetId: "1Coj3voJ6XiOMgdBO3M91DoDWrsSObPAxwOA5luBRHo0",
    });
  }

  return;
  let count_all = ar.length;
  let speeds = _.map(ar, "speed");
  speeds = _.filter(speeds, (e) => ![null, 0, undefined, NaN].includes(e));
  console.log(speeds);
  let count_valid = speeds.length;
  let min = _.min(speeds);
  let max = _.max(speeds);
  let med = calc_median(speeds);
  let avg = _.mean(speeds);
  let resp = [
    {
      count_all,
      count_valid,
      min,
      max,
      med,
      avg,
    },
  ];
  console.table(resp);
  if (process.argv.includes("write")) {
    await sheet_ops.sheet_print_ob(resp, {
      range: `Analyzer 6!$A8`,
      spreadsheetId: "1Coj3voJ6XiOMgdBO3M91DoDWrsSObPAxwOA5luBRHo0",
    });
  }
};

const run_34 = async () => {
  let init = "2022-01-01T00:00Z";
  let dist = 1000;
  for (let i = 0; i <= 7; i++) {
    let st = moment(init).add(i, "month").toISOString();
    let ed = moment(st).add(1, "month").toISOString();
    let doc = await zed_ch.db
      .collection("zed")
      .aggregate([
        {
          $match: {
            2: { $gte: st, $lte: ed },
            1: { $in: [dist, dist.toString()] },
          },
        },
        {
          $group: {
            _id: null,
            avg_finish: { $avg: "$7" },
          },
        },
      ])
      .toArray();
    let ea = doc[0];
    ea.month = i + 1;
    ea.dist = dist;
    console.table([ea]);
  }
};

const run_35 = async () => {
  let ar = await zed_db.db
    .collection("speed")
    .aggregate([
      { $sort: { speed: 1 } },
      { $match: { speed: { $ne: null } } },
      { $limit: 200 },
      { $project: { _id: 0, speed: 1 } },
    ])
    .toArray();
  console.table(ar);
  await sheet_ops.sheet_print_ob(ar, {
    spreadsheetId: "1Coj3voJ6XiOMgdBO3M91DoDWrsSObPAxwOA5luBRHo0",
    range: "BA!B2",
  });
};

const run_36 = async () => {
  console.log("run_36");
  /*
  select stats->'1600'->'median_speed_pd' as sp
  from horses
  -- where id=3312
  where stats->'1600'->'median_speed_pd' is not null
  order by stats->'1600'->'median_speed_pd' DESC
  limit 10
  */

  for (let [dist, ord, cell] of [
    [1600, "desc", "A3"],
    [1600, "asc", "B3"],

    // [1400, "desc", "C3"],
    // [1400, "asc", "D3"],

    // [1800, "desc", "E3"],
    // [1800, "asc", "F3"],
  ]) {
    // let dist = 1600;
    // let ord = "asc";
    let k = `${ord == "desc" ? "high" : "low"}_med_sp_${dist}`;
    // let cell = "B3";

    let ar = await knex_conn
      .select(
        knex_conn.raw(`
        id as hid,
        (
          COALESCE(cast(stats->'${dist}'->'firsts' as int),0) +
          COALESCE(cast(stats->'${dist}'->'seconds' as int),0) +
          COALESCE(cast(stats->'${dist}'->'thirds' as int),0) +
          COALESCE(cast(stats->'${dist}'->'fourths' as int),0) +
          COALESCE(cast(stats->'${dist}'->'other' as int),0)
        ) as n,
        stats->'${dist}'->'median_speed' as ${k}`)
      )
      .from("horses")
      // .where(knex_conn.raw(`id=3312`))
      .whereRaw(
        `
      (
        COALESCE(cast(stats->'${dist}'->'firsts' as int),0) +
        COALESCE(cast(stats->'${dist}'->'seconds' as int),0) +
        COALESCE(cast(stats->'${dist}'->'thirds' as int),0) +
        COALESCE(cast(stats->'${dist}'->'fourths' as int),0) +
        COALESCE(cast(stats->'${dist}'->'other' as int),0)
        ) >= 15 
        and stats->'${dist}'->'median_speed' != 'null'
      `
      )
      .orderBy(knex_conn.raw(`stats->'${dist}'->'median_speed'`), ord)
      .limit(100);
    ar = ar.filter((e, i) => {
      e["rat"] = e[k] * dist_factor[dist] * 1.45;
      return true;
    });
    console.table(ar.slice(0, 10));
    ar = ar.map((e) => ({
      [k]: e.rat,
    }));
    // console.table(ar);

    await sheet_ops.sheet_print_ob(ar, {
      spreadsheetId: "1Coj3voJ6XiOMgdBO3M91DoDWrsSObPAxwOA5luBRHo0",
      range: `BA!${cell}`,
    });
  }
};

const run_37 = async () => {
  let [st, ed] = get_date_range_fromto(-90, "days", 0, "days");
  console.log(st, ed);
  let ar = [];
  for (let dist of [1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600]) {
    let doc = await zed_ch.db
      .collection("zed")
      .aggregate([
        {
          $match: {
            1: dist,
            8: { $in: [6, 7] },
            2: { $gte: st, $lte: ed },
          },
        },
        {
          $project: {
            pos: "$8",
            time: "$7",
          },
        },
        {
          $group: {
            _id: "$pos",
            mean_time: { $avg: "$time" },
          },
        },
      ])
      .toArray();
    doc = _.chain(doc).keyBy("_id").mapValues("mean_time").value();
    let t6 = doc[6];
    let t7 = doc[7];
    let t67mean = doc[7];
    let ob = { dist, t6, t7, t67mean };
    ar.push(ob);
    console.log(ob);
  }
  await sheet_ops.sheet_print_ob(ar, {
    range: "Normalized times!A10",
    spreadsheetId: "1Coj3voJ6XiOMgdBO3M91DoDWrsSObPAxwOA5luBRHo0",
  });
};

const run_38 = async () => {
  let doc = preset_global;
  doc = _.chain(doc)
    .entries()
    .map(([k, row]) => {
      return { k, ...row };
    })
    .value();
  await sheet_ops.sheet_print_ob(doc, {
    range: "Normalized times!A10",
    spreadsheetId: "1Coj3voJ6XiOMgdBO3M91DoDWrsSObPAxwOA5luBRHo0",
  });
};

const run_39 = async () => {
  let rid = "6rGVigMv";
  let race = await zed_ch.db.collection("zed").find({ 4: rid }).toArray();
  race = cyclic_depedency.struct_race_row_data(race);
  race = _.keyBy(race, "place");
  let dist = race[1].distance;
  let t6 = race[6].finishtime;
  let t7 = race[7].finishtime;
  // console.table(race);
  const t67mean = _.mean([t6, t7]);
  const tmean = 95.65;
  console.log({ t6, t7 });
  console.log({ t67mean, tmean });
  let tadj = tmean - t67mean;
  let t1 = race[1].finishtime;
  let t1adj = t1 + tadj;
  let speed_init = ((dist / t1adj) * 60 * 60) / 1000;
  let speedadj = speed_init * 1.45 * dist_factor[dist];
  console.table([{ tadj, t1, t1adj, speed_init, speedadj }]);
};

const run_40 = async () => {
  let lim = 20;
  let [st, ed] = get_date_range_fromto(-90, "days", 0, "minutes");
  let ar = await zed_ch.db
    .collection("zed")
    .aggregate([
      { $match: { 2: { $gte: st, $lte: ed }, 24: { $ne: null } } },
      { $sort: { 24: 1 } },
      { $limit: lim },
      {
        $project: {
          _id: 0,
          hid: "$6",
          speed: "$24",
        },
      },
    ])
    .toArray();
  console.table(ar);
  if (true)
    await sheet_ops.sheet_print_ob(ar, {
      range: "speedrathl!D1",
      spreadsheetId: "1Coj3voJ6XiOMgdBO3M91DoDWrsSObPAxwOA5luBRHo0",
    });
};

const run_41 = async () => {
  let hid = 393589;
  let [st, ed] = get_date_range_fromto(-90, "days", 0, "minutes");

  for (let dist of [1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600]) {
    let ar = await zed_ch.db
      .collection("zed")
      .aggregate([
        {
          $match: {
            1: dist,
            6: hid,
            2: { $gte: st, $lte: ed },
            25: { $ne: null },
          },
        },
        { $sort: { 25: -1 } },
        { $limit: 1 },
        {
          $project: {
            _id: 0,
            hid: "$6",
            speed: "$25",
            adjtime: "$23",
            distance: "$1",
          },
        },
      ])
      .toArray();
    console.table(ar);
  }
};

const run_42 = async () => {
  let [st, ed] = get_date_range_fromto(-90, "days", 0, "minutes");
  const paid = 1;
  const cell = "A12";
  let ar = await zed_ch.db
    .collection("zed")
    .aggregate([
      { $match: { 2: { $gte: st, $lte: ed }, 8: 1 } },
      { $match: { 3: { [paid ? "$ne" : "$eq"]: "0.0" } } },
      // { $limit: 5 },
      { $group: { _id: "$5", avg_win_speedrat: { $avg: "$25" } } },
    ])
    .toArray();
  ar = _.sortBy(ar, "_id");
  // ar = struct_race_row_data(ar);
  console.table(ar);
  if (true) {
    await sheet_ops.sheet_print_ob(ar, {
      range: `WinSpeed!${cell}`,
      spreadsheetId: "1Coj3voJ6XiOMgdBO3M91DoDWrsSObPAxwOA5luBRHo0",
    });
  }
};

const run_43 = async () => {
  let [st, ed] = get_date_range_fromto(-90, "days", 0, "minutes");

  let ar = [];
  let hids = [67008];
  for (let hid of hids) {
    let rs = await zed_ch.db
      .collection("zed")
      .find(
        { 6: hid, 2: { $gte: st, $lte: ed } },
        { projection: { 8: 1, 25: 1 } }
      )
      .toArray();
    let speed_rat = _.max(_.map(rs, "25"));
    let avg_winspeed_rat = _.mean(_.map(_.filter(rs, { 8: 1 }), "25"));
    let median_speedrat = calc_median(_.map(rs, "25"));
    let ob = {
      hid,
      speed_rat,
      avg_winspeed_rat,
      median_speedrat,
    };
    console.table([ob]);
    ar.push(ob);
  }
  console.table(ar);
  if (true) {
    await sheet_ops.sheet_print_ob(
      ar,
      {
        range: `WinSpeed!${"F7"}`,
        spreadsheetId: "1Coj3voJ6XiOMgdBO3M91DoDWrsSObPAxwOA5luBRHo0",
      },
      false
    );
  }
};

const run_44 = async () => {
  let [st, ed] = get_date_range_fromto(-20, "days", 0, "minutes");
  let ex = 30 * 60;
  // let [st, ed] = ["2022-04-03T22:55:12.849Z", "2022-07-02T22:55:12.853Z"];

  console.log(st, ed);
  let redid = `races::run44:${st}:${ed}`;
  const get_races_fn = async () =>
    zed_ch.db
      .collection("zed")
      .aggregate([
        { $match: { 2: { $gte: st, $lte: ed }, 6: { $gte: 360000 }, 8: 1 } },
        {
          $project: {
            _id: 0,
            // date: "$2",
            rc: "$5",
            paid: { $cond: [{ $eq: ["$3", "0.0"] }, 0, 1] },
            // fee: "$3",
            rid: "$4",
            hid: "$6",
            speedrat: "$25",
          },
        },
        // { $limit: 500 },
        // { $match: { 3: { [paid ? "$ne" : "$eq"]: "0.0" } } },
      ])
      .toArray();
  let races = await rfn(redid, get_races_fn, ex, 1);
  console.table(races);

  const hid_stas_fn = async (hid) => {
    let rs = await zed_ch.db
      .collection("zed")
      .find(
        { 6: hid, 2: { $gte: st, $lte: ed } },
        { projection: { 8: 1, 25: 1 } }
      )
      .toArray();
    if (rs.length < 20) return null;
    let speedrat = _.max(_.map(rs, "25"));
    let avg_winspeedrat = _.mean(_.map(_.filter(rs, { 8: 1 }), "25"));
    let median_speedrat = calc_median(_.map(rs, "25"));
    let bestrat = (median_speedrat * 3 + speedrat * 1) / 4;
    let ob = {
      hid,
      speedrat,
      avg_winspeedrat,
      median_speedrat,
      bestrat,
    };
    return ob;
  };

  let ar = [];

  for (let { hid, rid, speedrat, rc, paid } of races) {
    let hob = await red.rfn(
      `hob:run_44:${hid}:${st}:${ed}`,
      () => hid_stas_fn(hid),
      ex,
      1
    );
    console.log(hid, jstr(hob));
    if (!hob) continue;
    let hbestrat = hob.bestrat;
    ar.push({ rid, rc, paid, hid, speedrat, hbestrat });
  }
  console.table(ar);
  let ob = [
    [0, 0],
    [1, 0],
    [2, 0],
    [3, 0],
    [4, 0],
    [5, 0],
    [6, 0],
    [99, 0],
    [1000, 0],

    [0, 1],
    [1, 1],
    [2, 1],
    [3, 1],
    [4, 1],
    [5, 1],
    [6, 1],
    [99, 1],
    [1000, 1],
  ].map(([rc, paid]) => {
    let filt = _.filter(ar, { rc, paid });
    let avgwinner_speedrat = _.meanBy(filt, "speedrat");
    let avg_best = _.meanBy(filt, "hbestrat");
    return { rc, paid, avgwinner_speedrat, avg_best };
  });
  console.table(ob);
  if (true) {
    await sheet_ops.sheet_print_ob(ob, {
      range: `WinSpeed!${"A44"}`,
      spreadsheetId: "1Coj3voJ6XiOMgdBO3M91DoDWrsSObPAxwOA5luBRHo0",
    });
  }
};

const run_45 = async () => {
  let lim = 500;
  let fin = [];
  let i = 42;
  for (let [rc, paid] of [
    [0, 0],
    [1, 0],
    [2, 0],
    [3, 0],
    [4, 0],
    [5, 0],
    [6, 0],
    [99, 0],
    [1000, 0],

    [0, 1],
    [1, 1],
    [2, 1],
    [3, 1],
    [4, 1],
    [5, 1],
    [6, 1],
    [99, 1],
    [1000, 1],
  ]) {
    // let [rc, dist, paid] = [1, "a", 1];
    let ar = await zed_ch.db
      .collection("zed")
      .aggregate([
        { $match: { 8: 1, 5: rc } },
        { $sort: { 2: -1 } },
        {
          $project: {
            _id: 0,
            raceid: "$4",
            hid: "$6",
            date: "$2",
            paid: { $cond: [{ $eq: ["$3", "0.0"] }, 0, 1] },
            class: "$5",
            dist: "$1",
          },
        },
        { $match: { paid: paid } },
        { $limit: lim },
      ])
      .toArray();
    let hids = _.map(ar, "hid");
    let speeds = await zed_db.db
      .collection("speed")
      .find(
        { hid: { $in: hids } },
        { projection: { _id: 0, hid: 1, speed: 1 } }
      )
      .toArray();
    speeds = _.chain(speeds).keyBy("hid").mapValues("speed").value();
    ar = ar.map((e) => {
      return {
        raceid: e.raceid,
        hid: e.hid,
        speedrat: speeds[e.hid],
        date: e.date,
        paid: e.paid,
        class: e.class,
        dist: e.dist,
      };
    });
    console.table(ar);
    let avg_speedrat = _.meanBy(ar, "speedrat");
    let ob = { rc, paid, avg_speedrat };
    console.log(ob);
    i += 2;
    if (true) {
      await sheet_ops.sheet_print_ob([ob], {
        range: `WinSpeed!G${i}`,
        spreadsheetId: "1Coj3voJ6XiOMgdBO3M91DoDWrsSObPAxwOA5luBRHo0",
      });
    }
    fin.push(ob);
  }

  console.table(fin);
};

const run_46 = async () => {
  console.log("run_46");
  let [st, ed] = get_date_range_fromto(-90, "days", 0, "minutes");
  let ar = await zed_ch.db
    .collection("zed")
    .aggregate([
      {
        $match: {
          2: {
            $gte: st,
            $lte: ed,
          },
          // 1: 1200,
          6: 425534,
        },
      },
      {
        $group: {
          _id: "$1",
          // ntimes: {
          //   $push: "$23",
          // },
          n: { $sum: 1 },
          mi: { $min: "$23" },
          mx: { $max: "$23" },
          med: {
            $accumulator: {
              init: `function () {
                return [];
              }`,
              accumulate: `function (bs, b) {
                return bs.concat(b);
              }`,
              accumulateArgs: ["$23"],
              merge: `function (bs1, bs2) {
                return bs1.concat(bs2);
              }`,
              finalize: `function (bs) {
                bs.sort(function (a, b) {
                  return a - b;
                });
                var mid = bs.length / 2;
                return mid % 1 ? bs[mid - 0.5] : (bs[mid - 1] + bs[mid]) / 2;
              }`,
              lang: "js",
            },
          },
        },
      },
    ])
    .toArray();
  console.table(ar);
  // let ob = [];
  // for (let e of ar) {
  //   e.ntimes = _.compact(e.ntimes);
  //   let mi = _.min(e.ntimes);
  //   let mx = _.max(e.ntimes);
  //   let med = calc_median(e.ntimes);
  //   let n = e.ntimes?.length;
  //   let count = parseInt(n / 12);
  //   ob.push({ dist: e._id, n, count, mi, med, mx });
  // }
  // ob = _.sortBy(ob, "dist");
  // console.table(ob);
  if (false) {
    await sheet_ops.sheet_print_ob(ob, {
      range: `Sheet4!${"A14"}`,
      spreadsheetId: "1WYzF7Unz-IPyJo9d2CFE07QAT-CRO60Z1GVbKSsOcSU",
    });
  }
};

const run_47 = async () => {
  let [hmi, hmx] = [370000, 430000];
  let ar = await zed_db.db
    .collection("horse_details")
    .aggregate([
      { $match: { hid: { $gte: hmi, $lte: hmx } } },
      {
        $project: {
          _id: 0,
          hid: 1,
          m: "$parents.mother",
          f: "$parents.father",
        },
      },
      ...ag_look("speed", "hid", "hid", "spdoc", false, {
        baby_sp: "$spdoc.speed",
      }),
      ...ag_look("speed", "m", "hid", "mspdoc", false, {
        m_sp: "$mspdoc.speed",
      }),
      ...ag_look("speed", "f", "hid", "fspdoc", false, {
        f_sp: "$fspdoc.speed",
      }),
      {
        $project: {
          _id: 0,
          hid: 1,
          baby_sp: 1,
          m: 1,
          m_sp: 1,
          f: 1,
          f_sp: 1,
        },
      },
      {
        $match: {
          baby_sp: { $ne: null },
          m_sp: { $ne: null },
          f_sp: { $ne: null },
        },
      },
      { $limit: 1000 },
    ])
    .toArray();
  // console.table(ar);
  const pars = {
    spreadsheetId: "1Coj3voJ6XiOMgdBO3M91DoDWrsSObPAxwOA5luBRHo0",
  };
  if (true) {
    for (let [c, k] of [
      ["A1", "hid"],
      ["B1", "baby_sp"],
      ["C1", "f"],
      ["D1", "f_sp"],
      ["E1", "m"],
      ["F1", "m_sp"],
    ]) {
      ob = _.chain(ar)
        .map((e) => _.pick(e, [k]))
        .value();
      console.log(c, k);
      // console.log(ob);
      if (true)
        await sheet_ops.sheet_print_ob(ob, {
          ...pars,
          range: `SpeedRatingCorrelation!${c}`,
        });
    }
  }
};

const run_48 = async () => {
  let cell = "I3";
  // let [st, ed] = get_date_range_fromto(-15, "days", 0, "minutes");
  let [st, ed] = ["2022-06-18T16:45:47.851Z", "2022-07-03T16:45:47.854Z"];
  console.log([st, ed]);
  let race_redid = `races::${st}:${ed}:all:1`;
  console.log(race_redid);
  let races = [];
  let exs = await red.rexists(race_redid);
  if (!exs) {
    races = await zed_ch.db
      .collection("zed")
      .aggregate([
        {
          $match: {
            2: { $gte: st, $lte: ed },
            // 1: 1600,
          },
        },
        {
          $project: {
            _id: 0,
            rc: "$5",
            paid: { $cond: [{ $eq: ["$3", "0.0"] }, 0, 1] },
            rid: "$4",
            hid: "$6",
            dist: "$1",
            time: "$7",
            place: "$8",
            adjtime: "$23",
            speed: "$24",
            speed_rat: "$25",
          },
        },
      ])
      .toArray();
    console.table(races.slice(0, 3));
    await red.rset(race_redid, races, 60 * 2);
    console.log("races.len::got", races.length);
  } else {
    races = await red.rget(race_redid);
    console.log("races.len::cache", races.length);
  }
  // return console.log("rdone");

  races = _.filter(races, { dist: 1600 });

  let ar = [];
  for (let [rc, paid] of [
    [0, 0],
    [1, 0],
    [2, 0],
    [3, 0],
    [4, 0],
    [5, 0],
    [6, 0],
    [99, 0],
    [1000, 0],

    [0, 1],
    [1, 1],
    [2, 1],
    [3, 1],
    [4, 1],
    [5, 1],
    [6, 1],
    [99, 1],
    [1000, 1],
  ]) {
    let filt = _.filter(races, { rc, paid });
    let count = filt.length;

    let mean = _.chain(filt).map("time").compact().mean().value();
    let mi = _.chain(filt).map("time").compact().min().value();
    let mx = _.chain(filt).map("time").compact().max().value();
    let sd = _.chain(filt)
      .map("time")
      .compact()
      .map((e) => Math.pow(mean - e, 2))
      .value();
    sd = _.sum(sd) / _.length(sd);

    let ob = {
      rc,
      paid,
      count,
      raw_time_mean_1600: mean,
      raw_time_mi_1600: mi,
      raw_time_mx_1600: mx,
      raw_time_sd_1600: sd,
    };
    console.log(ob);
    ar.push(ob);
  }
  console.table(ar);

  if (true) {
    ar = _.map(ar, (e) =>
      _.pick(e, [
        "raw_time_mean_1600",
        "raw_time_mi_1600",
        "raw_time_mx_1600",
        "raw_time_sd_1600",
      ])
    );
    await sheet_ops.sheet_print_ob(ar, {
      spreadsheetId: "1kUY3VjQeuPQi02VGVxxKgD9Ls_58lQsEENutokhT7jU",
      range: `Sheet1!${cell}`,
    });
  }
};

const tests = { run: run_48 };
module.exports = tests;
