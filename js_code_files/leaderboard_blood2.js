const _ = require("lodash");
const fetch = require("node-fetch");
const mongoose = require("mongoose");
const { init, zed_db, zed_ch, run_func } = require("./index-run");
const {
  write_to_path,
  read_from_path,
  struct_race_row_data,
  side_text,
  delay,
} = require("./utils");
const app_root = require("app-root-path");
const { dec, dec_per } = require("./utils");
const { initiate } = require("./odds-generator-for-blood2");

const leaderboard_download = async () => {
  await init();
  let st = 1;
  // let ed = 12000;
  let ed = 200000;
  let cs = 2000;
  let hids = new Array(ed - st + 1).fill(0).map((ea, idx) => st + idx);
  let i = 0;
  for (let chunk_hids of _.chunk(hids, cs)) {
    let docs_a = await zed_db.db
      .collection("rating_blood_dist")
      .find({ hid: { $in: chunk_hids } })
      .toArray();
    let this_hids = _.map(docs_a, "hid");
    if (_.isEmpty(this_hids)) {
      console.log("empty");
      continue;
    }
    let ob = {};
    for (let hid of this_hids) {
      let doc_a = _.find(docs_a, { hid });
      if (_.isEmpty(doc_a)) continue;
      // let dc = { hid };
      let dc = doc_a;
      ob[hid] = dc;
    }

    let [st_h, ed_h] = [chunk_hids[0], chunk_hids[chunk_hids.length - 1]];
    let file_path = `${app_root}/data/rating_blood_dist/rbd-${st_h}-${ed_h}.json`;
    write_to_path({ file_path, data: ob });
    console.log("chunk", chunk_hids[0], chunk_hids[chunk_hids.length - 1]);
    i++;
  }
  console.log("leaderboard_download horses_data");
};

const get_blood_str = (ob) => {
  try {
    let { cf, d, side, p12_ratio, rat, win_rate, flame_rate, rated_type } = ob;
    if (rated_type == "GH")
      return `${cf}-${d}-${dec(rat)}-${dec(win_rate)}-${dec(flame_rate)}`;
    return rated_type;
  } catch (err) {
    return "err";
  }
};

const get_downloaded_horses_data = async () => {
  let st = 1;
  // let ed = 12000;
  let ed = 200000;
  let cs = 2000;
  let hids = new Array(ed - st + 1).fill(0).map((ea, idx) => st + idx);
  let i = 0;
  let mapped = {};
  for (let chunk_hids of _.chunk(hids, cs)) {
    let [st_h, ed_h] = [chunk_hids[0], chunk_hids[chunk_hids.length - 1]];
    let file_path =
      `${app_root}/data/rating_blood_dist/rbd-${st_h}-${ed_h}.json` || {};
    let ob = read_from_path({ file_path });
    if (_.isEmpty(ob)) break;
    mapped = { ...mapped, ...ob };
  }
  console.log("got", _.keys(mapped).length);
  return mapped;
};
const generate_leaderboard_b2 = async (only = null, cs = 100) => {
  await initiate();
  await fix_empty_names();
  // if (down == 1) await leaderboard_download();
  // let mapped = await get_downloaded_horses_data();
  // console.log(mapped[3]);
  let dists = ["S", "M", "D", "All"];
  if (only) dists = [only];
  for (let dist of dists) {
    await generate_leaderboard_b2_each_dist(dist);
  }
};
// const generate_leaderboard_b2_each_dist = async ({ mapped, dist, cs }) => {
//   console.log("#DIST", dist);
//   mapped = _.chain(mapped)
//     .values()
//     .map((i) => {
//       if (_.isEmpty(i) || _.isEmpty(i[dist])) return null;
//       let { hid, name } = i;
//       let ob = { hid, name, ...i[dist] };
//       return ob;
//     })
//     .value();

//   let gh_s = _.filter(mapped, { rated_type: "GH" });
//   let ch_s = _.filter(mapped, { rated_type: "CH" });
//   let nr_s = _.filter(mapped, { rated_type: "NR" });

//   let ar = [];
//   ar = _.chain(gh_s)
//     .orderBy(
//       [
//         (i) => {
//           return i.cf;
//         },
//         (i) => {
//           return +i.rat;
//         },
//         (i) => {
//           return +i.win_rate;
//         },
//         (i) => {
//           return +i.flame_rate;
//         },
//       ],
//       ["asc", "desc", "desc", "desc"]
//     )
//     .map((i, idx) => {
//       let str = get_blood_str(i);
//       return { ...i, rank: idx + 1, str };
//     })
//     .value();
//   ar = ar.concat(_.map(ch_s, (i) => ({ ...i, rank: null })));
//   ar = ar.concat(_.map(nr_s, (i) => ({ ...i, rank: null })));
//   console.table(
//     _.chain(ar)
//       .map((i) => {
//         return { ...i };
//       })
//       .value()
//       .slice(0, 100)
//   );

//   let leader_ar = _.map(ar.slice(0, 100), (i) => {
//     let { rank, hid, cf, d, str, name } = i;
//     return {
//       hid,
//       name,
//       rank,
//       horse_rating: {
//         cf,
//         d,
//         str,
//         type: dist == "All" ? "all" : "dist",
//       },
//     };
//   });
//   let now = Date.now();
//   let leader_doc = {
//     id: `leaderboard-${dist}`,
//     date: now,
//     date_str: new Date(now).toISOString(),
//     db_date: new Date(now).toISOString(),
//     "leaderboard-dist": dist.toString(),
//     leaderboard: leader_ar,
//   };
//   // console.log(leader_doc);
//   await zed_db.db
//     .collection("leaderboard")
//     .updateOne({ id: leader_doc.id }, { $set: leader_doc }, { upsert: true });
//   console.log("written", dist, "leaderboard\n----------------\n");

//   let bulk = [];
//   await zed_db.db
//     .collection("rating_blood_dist")
//     .updateMany({}, { $set: { [`${dist}.rank`]: null } });
//   console.log("cleared, ranks");
//   let to_upd = _.filter(ar, (i) => i.rank !== null);
//   console.log("need to write ranks", to_upd.length);
//   for (let doc of to_upd) {
//     let { hid, rank } = doc;
//     if (!hid) continue;
//     bulk.push({
//       updateOne: {
//         filter: { hid },
//         update: { $set: { [`${dist}.rank`]: rank } },
//       },
//     });
//   }
//   let i = 0;
//   for (let mini_bulk of _.chunk(bulk, cs)) {
//     if (_.isEmpty(bulk)) continue;
//     await zed_db.db.collection("rating_blood_dist").bulkWrite(mini_bulk);
//     i += mini_bulk.length;
//     console.log("rating_blood_dist ranks:", dec_per(i, bulk.length));
//   }
// };

const leader_query = (dist = null, need_details = 0, limit = null) => {
  if (!dist) [{ $match: { hid: -1 } }];
  let ar = [
    { $match: { [`${dist}.rated_type`]: "GH" } },
    {
      $sort: {
        [`${dist}.cf`]: 1,
        [`${dist}.rat`]: -1,
        [`${dist}.win_rate`]: -1,
        [`${dist}.flame_rate`]: -1,
      },
    },
    ...(limit ? [{ $limit: limit }] : []),
    {
      $project: {
        _id: 0,
        hid: 1,
        name: 1,
        ...(need_details ? { [dist]: 1 } : {}),
        // [`${dist}.cf`]: 1,
        // [`${dist}.d`]: 1,
        // [`${dist}.rat`]: 1,
        // [`${dist}.win_rate`]: 1,
        // [`${dist}.flame_rate`]: 1,
        // [`${dist}.rank`]: 1,
        // [`${dist}.rated_type`]: 1,
      },
    },
  ];
  return ar;
};

const generate_leaderboard_b2_each_dist = async (dist) => {
  if (!dist) return;
  console.log(dist);
  let docs = await zed_db.db
    .collection("rating_blood_dist")
    .aggregate(leader_query(dist, 1, 100))
    .toArray();
  let leader_ar = _.map(docs, (i, idx) => {
    let { hid, name } = i;
    let { cf, d } = i[dist];
    let str = get_blood_str(i[dist]);
    let rank = idx + 1;
    return {
      hid,
      name,
      rank,
      horse_rating: {
        cf,
        d,
        str,
        type: dist == "All" ? "all" : "dist",
      },
    };
  });
  let now = Date.now();
  let leader_doc = {
    id: `leaderboard-${dist}`,
    date: now,
    date_str: new Date(now).toISOString(),
    db_date: new Date(now).toISOString(),
    "leaderboard-dist": dist.toString(),
    leaderboard: leader_ar,
  };
  console.table(leader_ar);
  await zed_db.db
    .collection("leaderboard")
    .updateOne({ id: leader_doc.id }, { $set: leader_doc }, { upsert: true });
  console.log(dist, "done");

  await write_ranks(dist);
};

const write_ranks = async (dist) => {
  try {
    let docs = await zed_db.db
      .collection("rating_blood_dist")
      .aggregate(leader_query(dist, 0, null));
    let i = 0;
    let cur = docs;
    let last_cur;
    await zed_db.db
      .collection("rating_blood_dist")
      .updateOne(
        { [`${dist}.rated_type`]: { $ne: "GH" } },
        { $set: { [`${dist}.rank`]: null } }
      );
    while (true) {
      let last_cur = _.cloneDeep(cur);
      try {
        if (!cur.hasNext()) break;
        let doc = await cur.next();
        let rank = ++i;
        // if (i % 100 == 0) await delay(2000);
        // if (i % 100 == 0)
        console.log(rank, doc);
        await zed_db.db
          .collection("rating_blood_dist")
          .updateOne({ hid: doc.hid }, { $set: { [`${dist}.rank`]: rank } });
        if (i % 10 == 0) await delay(200);
      } catch (err) {
        cur = last_cur;
        console.log("...err");
      }
    }
  } catch (err) {
    console.log(err);
    return;
  }
};

const fix_empty_names = async () => {
  console.log("fix_empty_names");
  let docs = await zed_db.db
    .collection("rating_blood2")
    .find({ name: { $exists: false } }, { projection: { _id: 0, hid: 1 } })
    .toArray();
  let hids = _.map(docs, "hid");

  let docs_h = await zed_db.db
    .collection("horse_details")
    .find({ hid: { $in: hids } }, { projection: { _id: 0, hid: 1, name: 1 } })
    .toArray();
  console.log(docs.length, "names to fix");
  let names_ob = _.chain(docs_h)
    .map((i) => {
      let { hid, name } = i;
      return [hid, name];
    })
    .fromPairs()
    .value();
  let bulk = [];
  for (let hid of hids) {
    let name = names_ob[hid];
    if (!name) continue;
    bulk.push({
      updateOne: {
        filter: { hid },
        update: { $set: { name } },
      },
    });
  }
  if (!_.isEmpty(bulk))
    await zed_db.db.collection("rating_blood2").bulkWrite(bulk);
  console.log(bulk.length, "fixed");
};

const runner = async () => {
  await generate_leaderboard_b2();
  console.log("done");
};
// runner();

module.exports = {
  generate_leaderboard_b2,
  generate_leaderboard_b2_each_dist,
  leaderboard_download,
};
