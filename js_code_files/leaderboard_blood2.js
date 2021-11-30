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
const generate_leaderboard_b2 = async (only = null, cs = 100) => {
  await initiate();
  await fix_empty_names();
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

const leader_query = (
  dist = null,
  need_details = 0,
  limit = null,
  skip = null
) => {
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
    ...(skip ? [{ $skip: skip }] : []),
    {
      $project: {
        _id: 0,
        hid: 1,
        name: 1,
        ...(need_details
          ? {
              name: 1,
              [dist]: 1,
            }
          : {}),
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

const generate_leaderboard_b2_each_dist = async (dist, cs = 20) => {
  await init();
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

  await leader_write_ranks_each_dist(dist, cs);
};

const leader_write_ranks_each_dist = async (dist, cs = 20) => {
  try {
    await init();
    // let d = await zed_db.db
    //   .collection("rating_blood_dist")
    //   .find(
    //     { [`${dist}.rated_type`]: "GH" },
    //     { projection: { _id: 0, hid: 1 } }
    //   )
    //   .toArray();
    // console.log(d.length);
    // return;

    let limit = 1e14;
    let skip = 0;
    let docs = await zed_db.db
      .collection("rating_blood_dist")
      .aggregate(leader_query(dist, 0, limit, skip));
    let cur = docs;
    let ar = await cur.toArray();
    ar = ar.map((item, idx) => {
      let rank = idx + skip + 1;
      return { rank, ...item };
    });
    console.log("GH: ", ar.length);

    await zed_db.db
      .collection("rating_blood_dist")
      .updateMany(
        { [`${"S"}.rank`]: { $ne: "GH" } },
        { $set: { [`${"S"}.rank`]: null } }
      );
    console.table("cleared ranks non-GH");

    ar = _.compact(ar);
    for (let chunk of _.chunk(ar, cs)) {
      let mini = [];
      for (let i of chunk) {
        if (_.isEmpty(i)) continue;
        console.log(`#${i.rank} @ ${dist}`, "hid:", i.hid);
        mini.push({
          updateOne: {
            filter: { hid: i.hid },
            update: { $set: { [`${dist}.rank`]: i.rank } },
          },
        });
      }
      await zed_db.db.collection("rating_blood_dist").bulkWrite(mini);
      console.log("done bulk", mini.length);
    }
    console.log("completed ranks", dist);
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
  await init();
  // await generate_leaderboard_b2();
  await zed_db.db
    .collection("rating_blood_dist")
    .updateMany({}, { $set: { [`${"S"}.rank`]: null } });

  console.log("done");
};
// runner();

module.exports = {
  generate_leaderboard_b2,
  generate_leaderboard_b2_each_dist,
  leader_write_ranks_each_dist,
};
