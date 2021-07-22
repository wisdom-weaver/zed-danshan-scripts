const _ = require("lodash");
const { MongoClient } = require("mongodb");
const mongoose = require("mongoose");
const prompt = require("prompt-sync")();
const fetch = require("node-fetch");
const app_root = require("app-root-path");

const {
  get_races_of_hid,
  calc_blood_hr,
  get_details_of_hid,
  mx,
} = require("./index-odds-generator");
const { run_func } = require("./index-run");
const { calc_avg, write_to_path, read_from_path } = require("./utils");
let zed_db = mongoose.connection;

const compare_horses_on_common_races_avgs = async ({ hid1, hid2, dist }) => {
  let race1 = await get_races_of_hid(hid1);
  race1 = _.keyBy(race1, "raceid");
  let keys1 = Object.keys(race1);
  let race2 = await get_races_of_hid(hid2);
  race2 = _.keyBy(race2, "raceid");
  let keys2 = Object.keys(race2);
  let comm_rids = _.intersection(keys1, keys2);
  comm_rids = comm_rids.filter((rid) => {
    if (race1[rid].distance !== race2[rid].distance) return false;
    if (dist != "All" && race1[rid].distance != dist) return false;
    return true;
  });
  let odds_table = comm_rids.map((rid) => ({
    odd1: race1[rid]?.odds,
    odd2: race2[rid]?.odds,
  }));
  const avg1 = calc_avg(odds_table.map((ea) => ea.odd1));
  const avg2 = calc_avg(odds_table.map((ea) => ea.odd2));
  return { avg1, avg2 };
};

const reorder_based_commom_races_avgs = async ({ ar, dist }) => {
  for (let i = 0; i < ar.length - 1; i++) {
    let a = ar[i];
    let b = ar[i + 1];

    const { hid: hid1 } = a;
    const { hid: hid2 } = b;
    const comp_ob = await compare_horses_on_common_races_avgs({
      hid1,
      hid2,
      dist,
    });
    const { avg1, avg2 } = comp_ob;
    let check = parseFloat(avg1) - parseFloat(avg2) > 0.5;
    let dec2 = (n) => parseFloat(n).toFixed(2);
    if (check) {
      ar[i] = b;
      ar[i + 1] = a;
    }
  }
  return ar;
};

const filter_hids = (hids) => {
  let bads = [15812, 15745];
  return hids.filter((hid) => !bads.includes(hid));
};

const get_all_ratings_of_hid = async (hid) => {
  let { odds_live } =
    (await zed_db.db.collection("odds_live").findOne({ hid })) || {};
  if (_.isEmpty(odds_live) || _.isEmpty(_.compact(_.values(odds_live))))
    return null;
  let dists = [1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600];
  let hrs = [];
  for (let i in dists)
    hrs[i] = await calc_blood_hr({
      odds_live,
      races_n: 1,
      override_dist: dists[i],
    });
  // console.log(hrs);
  hrs = _.keyBy(hrs, "d");
  hrs["All"] = await calc_blood_hr({ hid, odds_live, races_n: 1 });
  hrs["All"] = { ...hrs["All"], type: "all" };
  // console.log(hrs);
  return hrs;
};

const get_horse_rating_ob_for_hids = async (hids) => {
  let all_ar = [];
  for (let chunk of _.chunk(hids, 50)) {
    let sub = await Promise.all(
      chunk.map((hid) => {
        return get_all_ratings_of_hid(hid).then((horse_rating_ob) => {
          if (_.isEmpty(horse_rating_ob)) return null;
          else {
            let ob = { hid, horse_rating_ob };
            console.log("got", hid);
            return ob;
          }
        });
      })
    );
    all_ar = [...all_ar, ..._.compact(sub)];
  }
  all_ar = _.compact(all_ar);
  return all_ar;
};

const get_details_ob_for_hids = async (hids) => {
  let all_ar = [];
  for (let chunk of _.chunk(hids, 50)) {
    let sub = await Promise.all(
      chunk.map((hid) => {
        console.log("det", hid);
        try {
          return get_details_of_hid(hid);
        } catch (err) {
          console.log("err", hid);
          return { name: "####" };
        }
      })
    );
    all_ar = [...all_ar, ..._.compact(sub)];
  }
  all_ar = _.compact(all_ar);
  return all_ar;
};

const generate_all_dists_leaderboard = async () => {
  let dists = ["All", 1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600];
  // let dists = ["All"];
  // let dists = [1000]
  // if (!dists.includes(dist)) return;
  let hids = new Array(mx + 1).fill(0).map((a, i) => i);
  // let hids = []
  hids = filter_hids(hids);
  console.log(hids);
  // console.log({ hids });
  let all_ar = await get_horse_rating_ob_for_hids(hids);

  console.log(all_ar.length);

  for (let dist of dists) {
    let ar = all_ar.map(({ hid, horse_rating_ob = {} }) => {
      let hr = horse_rating_ob[dist];
      if (_.isEmpty(hr)) return null;
      if (hr.rated_type != "GH") return null;
      return { hid, horse_rating: hr };
    });
    ar = _.compact(ar);
    // ar = ar.filter(
    //   (item) => item?.horse_rating?.cf != null && item?.horse_rating?.cf != "na"
    // );
    ar = _.sortBy(ar, (item) => item.horse_rating?.cf || "na");
    ar = _.groupBy(ar, (item) => item.horse_rating?.cf || "na");
    let keys = Object.keys(ar).sort();
    let new_ar = [];
    for (let key of keys) {
      ar[key] = _.sortBy(ar[key], (o) => +o.horse_rating?.med || -1);
    }
    ar = _.flatten(_.values(ar));
    let top_1000 = ar.slice(0, 1000).map((e) => e.hid);
    await zed_db.db
      .collection("leaderboard")
      .updateOne(
        { id: `top_1000_${dist}` },
        { $set: { id: `top_1000_${dist}`, dist: dist, top_1000 } },
        { upsert: true }
      );
    console.log(`wrote top_1000_${dist}`);

    ar = ar.slice(0, 100);
    let hids = ar.map((ea) => ea.hid);

    if (dist != "All") hids = await compare_heads(hids, dist);
    await zed_db.db
      .collection("leaderboard")
      .updateOne(
        { id: `heads_top_100_${dist}` },
        { $set: { id: `heads_top_100_${dist}`, dist: dist, top_100: hids } },
        { upsert: true }
      );
    console.log(`wrote heads_top_100_${dist}`);
    let det_ar = await get_details_ob_for_hids(hids);

    ar = hids.map((hid, idx) => ({
      hid,
      rank: idx + 1,
      ...(_.find(ar, { hid }) || {}),
      name: det_ar[idx]?.name,
    }));
    console.log({ ar });

    let date = new Date();
    let id = `leaderboard-${dist}`;
    let ob = {
      id,
      date: date.getTime(),
      date_str: date.toISOString(),
      "leaderboard-dist": dist,
      leaderboard: ar,
    };
    console.log(ar);
    await zed_db.db
      .collection("leaderboard")
      .updateOne({ id }, { $set: ob }, { upsert: true });
    console.log("dist", dist, "complete");
    write_to_path({
      file_path: `${app_root}/test/leader-${dist}.json`,
      data: ob,
    });
  }
  console.log("#finished all dists leaderboard generation");
  return;
};

const repair_leaderboard = async () => {
  let dists = ["All", 1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600].map(
    (ea) => ea.toString()
  );
  // let dists = [1400];
  for (let dist of dists) {
    let lhidsdoc = await zed_db.db
      .collection("leaderboard")
      .findOne({ id: `top_1000_${dist}` });
    let top_100 = (lhidsdoc?.top_1000 || []).slice(0, 100);
    console.log(top_100);
    // top_100 = top_100.slice(0, 20);
    if (dist != "All") top_100 = await compare_heads(top_100, dist);
    let all_ar = await get_horse_rating_ob_for_hids(top_100);
    // console.log(all_ar.slice(0, 10));
    let det_ar = await get_details_ob_for_hids(top_100);
    all_ar = all_ar.map((ea, i) => ({
      rank: i + 1,
      hid: ea.hid,
      horse_rating: ea.horse_rating_ob[dist],
      name: det_ar[i].name,
    }));
    // console.log(all_ar[0]);
    console.table(all_ar);

    let date = new Date();
    let id = `leaderboard-${dist}`;
    let ob = {
      id,
      date: date.getTime(),
      date_str: date.toISOString(),
      "leaderboard-dist": dist,
      leaderboard: all_ar,
    };
    console.log(ob);
    await zed_db.db
      .collection("leaderboard")
      .updateOne({ id }, { $set: ob }, { upsert: true });

    console.log("#done leaderboard ", dist);
  }
};

const check_h2_better_h1 = async (hid1, hid2, dist) => {
  // console.log("head to head", hid1, hid2);
  let r1 = await get_races_of_hid(hid1);
  r1 = r1.filter((e) => e.distance == dist);
  let r2 = await get_races_of_hid(hid2);
  r2 = r2.filter((e) => e.distance == dist);
  // console.log({ r2 });
  let comm = _.intersectionBy(r1, r2, "raceid");
  comm = comm.map(({ raceid }) => ({
    raceid,
    odds1: _.find(r1, { raceid }).odds,
    odds2: _.find(r2, { raceid }).odds,
  }));
  const avg1 = calc_avg(_.map(comm, "odds1"));
  const avg2 = calc_avg(_.map(comm, "odds2"));
  // console.log(comm.map((e) => e.raceid));
  // console.table(comm);
  // console.log("dist =>", dist);
  // console.log(hid1, "=>", avg1);
  // console.log(hid2, "=>", avg2);
  console.log("head to head", hid1, hid2, "diff: ", avg1 - avg2);
  // let dec2 = (n) => parseFloat(n).toFixed(2);
  // let b = avg2 < avg1 ? hid2 : hid1;
  return avg1 - avg2;
  // let data = read_from_path({
  //   file_path: `${app_root}/data/1400-data-table.json`,
  // });
  // data = data?.table || {};
  // return data[hid2][hid1];
};

const compare_heads = async (hids = [], dist) => {
  let arr = [...hids];
  // console.log(hids);
  let n = hids.length;
  for (let i = 1; i < n; ++i) {
    let key = arr[i];
    let j = i - 1;
    while (j >= 0 && (await check_h2_better_h1(arr[j], key, dist)) >= 0) {
      // console.log(`${i}th`, key, " & ", `${j}th`, arr[j]);
      arr[j + 1] = arr[j];
      j = j - 1;
    }
    arr[j + 1] = key;
    // console.log(arr.toString());
  }
  return arr;
};

const run_leaderboard = () => {
  run_func(generate_all_dists_leaderboard);
};

const run_repair_leaderboard = () => {
  run_func(repair_leaderboard);
};

const test = async () => {
  // let hids = [
  //   1892, 325, 10119, 296, 26122, 701, 980, 18904, 13089, 17345, 35036, 412,
  //   463, 3634, 16451, 12968, 7711, 8607, 13389, 7090, 441, 939, 23272, 13287,
  //   1637, 1978, 2009, 14867, 26072, 36685, 916, 26113, 17429, 12210, 5, 918, 79,
  //   13001, 12315, 237, 22081, 1653, 13575, 131, 3136, 6341, 24564, 29470, 15045,
  //   26506, 17565, 6366, 13613, 143, 617, 12839, 18919, 13687, 23832, 7700, 1691,
  //   274, 975, 23527, 11040, 17888, 20742, 1446, 10123, 24225, 5466, 22850,
  //   20298, 864, 197, 13935, 12194, 26574, 5245, 13854, 27007, 3607, 5535, 13405,
  //   6423, 1252, 13205, 1226, 7995, 22431, 16119, 3981, 14822, 13204, 26102,
  //   6342, 13743, 900, 12261, 13327,
  // ];
  // let dist = 1400;
  // hids = hids.slice(0, 3);
  // hids = await compare_heads(hids, dist);
  // console.log(hids);
  // let ar = new Array(hids.length).fill({});
  // for (let i in ar) {
  //   let hid = hids[i];
  //   console.log(i, hid);
  //   let ob = {};
  //   let rank = parseInt(i) + 1;
  //   let det = await zed_db.db.collection("rating_blood").findOne({ hid: hid });
  //   let name = det.details.name;
  //   ar[i] = { hid, rank, name };
  // }
  // console.log(ar);
  // console.table(ar);
  // let lhidsdoc = await zed_db.db
  //   .collection("leaderboard")
  //   .findOne({ id: `top_1000_${dist}` });
  // let top_100 = (lhidsdoc?.top_1000 || []).slice(0, 15);
  // console.log(top_100);

  // let top_100 = [35036, 412, 463, 3634];
  // let top_100 = [35036, 412, 463, 3634];
  // let top_100 = [3634, 463];
  // let top_100 = [412, 463];

  // top_100 = await compare_heads(top_100, dist);

  // let all_ar = await get_horse_rating_ob_for_hids(top_100);
  // let det_ar = await get_details_ob_for_hids(top_100);

  // let ob = top_100.map((hid, i) => {
  //   // console.log(all_ar[i]);
  //   return {
  //     rank: i + 1,
  //     hid,
  //     name: det_ar[i].name,
  //     ...all_ar[i].horse_rating_ob[dist],
  //   };
  // });

  // console.log("sorted", top_100);
  // console.table(ob);

  // console.log(await check_h2_better_h1(325, 10119, 1400));
  await repair_leaderboard();
};

const test2 = async () => {
  let hids = [
    1892, 325, 10119, 296, 26122, 701, 980, 18904, 13089, 17345, 35036, 412,
    463, 3634, 16451, 12968, 7711, 8607, 13389, 7090, 441, 939, 23272, 13287,
    1637, 1978, 2009, 14867, 26072, 36685, 916, 26113, 17429, 12210, 5, 918, 79,
    13001, 12315, 237, 22081, 1653, 13575, 131, 3136, 6341, 24564, 29470, 15045,
    26506, 17565, 6366, 13613, 143, 617, 12839, 18919, 13687, 23832, 7700, 1691,
    274, 975, 23527, 11040, 17888, 20742, 1446, 10123, 24225, 5466, 22850,
    20298, 864, 197, 13935, 12194, 26574, 5245, 13854, 27007, 3607, 5535, 13405,
    6423, 1252, 13205, 1226, 7995, 22431, 16119, 3981, 14822, 13204, 26102,
    6342, 13743, 900, 12261, 13327,
  ];
  console.log(hids.length);
  // hids = hids.slice(0, 3);
  let n = hids.length;
  let a = {};
  // for (let i in hids) {
  // //   if (i <= 82) continue;
  //   a[hids[i]] = {};
  //   console.log("----------\n", hids[i]);
  //   for (let h of hids) {
  //     a[hids[i]][h] = "";
  //     if (h == hids[i]) {
  //       a[hids[i]][h] = "NA";
  //       continue;
  //     }
  //     let v = await check_h2_better_h1(hids[i], h, 1400);
  //     a[hids[i]][h] = v;
  //   }
  //   write_to_path({
  //     file_path: `${app_root}/data/1400-data-table-${i}-${hids[i]}.json`,
  //     data: { table: a[hids[i]] },
  //   });
  // }
  for (let i in hids) {
    console.log("----------\n", hids[i]);
    let h_ob = read_from_path({
      file_path: `${app_root}/data/1400-data-table-${i}-${hids[i]}.json`,
    });
    let ob = _.entries(h_ob.table).map(([k, v]) => {
      let idx = hids.findIndex((ea) => ea == k) + 1;
      // diff:-2.73| b:325
      // console.log(v);
      if (v != "NA") {
        let [diff, b] = v.split("| ");
        b = b.slice(2);
        diff = diff.slice(5);
        diff = parseFloat(diff);
        let s = Math.sign(diff);

        s =
          (s == 1 && `+${Math.abs(s)}`) || (s == -1 && `-${Math.abs(s)}`) || 0;
        if (s == 0) v = 0;
        else v = `${diff}`;
      } else v = 0;
      // return [`${pad(idx)}-${pad(k, 5)}`, v];
      return [k, v];
    });
    ob = ob.map(([k, v]) => ({ k, v }));
    ob = _.sortBy(ob, "k");
    ob = ob.map(({ k, v }) => [k, v]);

    ob = Object.fromEntries(ob);
    let ii = parseInt(i) + 1;
    // a[`${pad(ii)}-${pad(hids[i], 5)}`] = ob;
    a[hids[i]] = ob;
  }
  console.table(a);
  write_to_path({
    file_path: `${app_root}/data/1400-data-table.json`,
    data: { table: a },
  });

  console.log("\n\n");
  const value = a;
  let result = {};
  _.forOwn(value, (v, c) =>
    _.forOwn(v, (w, r) => ((result[r] = result[r] || {})[c] = w))
  );
  console.table(result);
};

let pad = (n, l = 4) => {
  let zsl = l - n.toString().length;
  let zs = new Array(zsl).fill(0).join("");
  return `${zs}${n}`;
};

const run_test = async () => {
  run_func(test);
};

const run_test2 = async () => {
  run_func(test2);
};

module.exports = {
  generate_all_dists_leaderboard,
  run_leaderboard,
  run_repair_leaderboard,
  run_test,
  run_test2,
};
