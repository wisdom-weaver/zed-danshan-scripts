const { off } = require("superagent");
const { zed_ch } = require("../connection/mongo_connect");
const { get_entryfee_usd } = require("../utils/base");
const cyclic_depedency = require("../utils/cyclic_dependency");
const { get_fee_tag, nano, iso, getv, cdelay } = require("../utils/utils");
const races_base = require("./races_base");
const _ = require("lodash");
const bulk = require("../utils/bulk");
const norm_time_s = require("./norm_time");

const fix_race_usd = async (rid) => {
  let race = await zed_ch.db
    .collection("zed")
    .find(
      { 4: rid },
      {
        projection: {
          _id: 0,
          2: 1,
          3: 1, // entryfee,
          4: 1,
          6: 1,
          20: 1, // prize,
        },
      }
    )
    .toArray();
  race.map((e) => {
    let date = e[2];
    let entryfee = e[3];
    let prize = e[20];
    let entryfee_usd = (e["18"] = get_entryfee_usd({ fee: entryfee, date }));
    let fee_tag = (e["19"] = get_fee_tag(entryfee_usd));
    let prize_usd = (e["21"] = get_entryfee_usd({ fee: prize, date }));
  });
  // console.table(race);
  return race;
};

const usd_price_fixer = async (st, ed) => {
  let off = 10 * 60 * 1000;
  for (let now = nano(st); now < nano(ed); ) {
    let now_st = iso(now);
    let now_ed = iso(now + off);
    console.log(now_st, "->", now_ed);
    try {
      // let rids = await races_base.get_zed_rids_only(now_st, now_ed);
      let rids = await zed_ch.db
        .collection("zed")
        .aggregate([
          {
            $match: {
              2: { $gte: now_st, $lte: now_ed },
            },
          },
          {
            $match: {
              $or: [{ 4: { $in: ["99", "1000"] } }, { 3: { $ne: "0.0" } }],
            },
          },
          { $project: { _id: 0, 4: 1 } },
        ])
        .toArray();
      rids = _.chain(rids).map("4").compact().uniq().value();
      console.log("rids", rids.length);
      console.log(rids);
      for (let chu of _.chunk(rids, 10)) {
        let ea = await Promise.all(chu.map((rid) => fix_race_usd(rid)));
        let upd = _.flatten(ea);
        // console.table(upd);
        let resp = await bulk.push_zed_ch_races(upd);
        console.log(upd.length, "docs update");
      }
    } catch (err) {
      console.log("ERROR", err.message);
    }
    now = Math.min(nano(now_ed), nano(ed));
  }
};

const fix_race_norm = async (rid) => {
  let race = await zed_ch.db
    .collection("zed")
    .find(
      { 4: rid },
      {
        projection: {
          _id: 0,
          4: 1,
          6: 1,
          1: 1,
          7: 1,
        },
      }
    )
    .toArray();
  race = norm_time_s.eval(race, {
    time_key: "7",
    dist_key: "1",
    adjtime_key: "23",
    race_time_key: "26",
  });
  // console.table(race);
  return race;
};

const update_race_prizes = async ([st, ed]) => {
  st = iso(st);
  ed = iso(ed);
  let cursor = null;

  const wri_bulk = async (ar) => {
    let bulk = ar.map((e) => {
      if (_.isEmpty(e)) return null;
      return {
        updateOne: {
          filter: { 4: e[4], 6: e[6] },
          update: { $set: { ...e } },
          upsert: true,
        },
      };
    });
    bulk = _.compact(bulk);
    await zed_ch.db.collection("zed").bulkWrite(bulk);
  };

  do {
    let raw = await races_base.get_zed_raw_data(st, ed, cursor);
    // console.log(raw);
    cursor = getv(raw, "pageInfo.endCursor");
    let races = raw.racesData;
    if (_.isEmpty(races)) {
      console.log("empty");
      continue;
    }
    console.log("races", _.keys(races).length);
    let ar = [];
    if (_.keys(races).length > 0) {
      for (let [rid, rs] of _.entries(races)) {
        if (_.isEmpty(rs)) continue;
        let rpay = getv(_.values(rs), `0.27`);
        console.log("\trace:", rid, rpay, "pays");
        let nrs = _.chain(rs)
          .values()
          .map((e) => _.pick(e, ["4", "6", "20", "21", "27"]))
          .value();
        // console.log(nrs);
        ar.push(nrs);
      }
    }
    ar = _.flatten(ar);
    // console.log(ar[0]);
    await wri_bulk(ar);
    console.log("updated", ar.length, "race rows");
    // await cdelay(10000);
    if (getv(raw, "pageInfo.hasNextPage") == false) break;
  } while (true);
  return console.log("ended");
};

const test = async () => {
  console.log("\n\ntest");
  const rid = "Wa2LOCNe";
  let r = await fix_race_norm(rid);
  console.table(r);
};

const main_runner = async () => {
  let [_node, _cfile, arg1, arg2, arg3, arg4, arg5] = process.argv;
  if (arg3 == "race_usd_vals") {
    let [st, ed] = [arg4, arg5];
    console.log("race_usd_vals", st, ed);
    await usd_price_fixer(st, ed);
  }
  if (arg3 == "update_race_prizes") {
    let st = arg4;
    let ed = arg5 ?? iso();
    await update_race_prizes([st, ed]);
  }
  if (arg3 == "test") {
    await test();
  }
};

const race_fixers = {
  main_runner,
};
module.exports = race_fixers;
