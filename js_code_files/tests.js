const { generate_blood_mapping } = require("./index-odds-generator");
const {
  compare_heads,
  check_h2_better_h1,
  best_in_battlefield,
  had_last_race_in_days,
} = require("./leaderboard-generator");
const { init, zed_db, zed_ch } = require("./index-run");
const _ = require("lodash");
const mongoose = require("mongoose");
const { write_to_path, read_from_path } = require("./utils");
const app_root = require("app-root-path");
const { MongoClient } = require("mongodb");
const fetch = require("node-fetch");
const { download_eth_prices } = require("./base");

const test1 = async () => {
  await init();
  // let ob = [
  //   { a: null, b: 1 },
  //   { a: 2, b: 3 },
  //   { a: null, b: 2 },
  //   { a: null, b: 4 },
  // ];

  // let datas = await zed_db.db.collection("leaderboard").find({}).toArray();
  // for (let data of datas) {
  //   console.log("writing ", data.id, "...");
  //   write_to_path({
  //     file_path: `${app_root}/backup/leaderboard/${data.id}.json`,
  //     data,
  //   });
  // }

  let dists = ["All", 1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600];
  for (let dist of dists) {
    let id = `leaderboard-${dist}`;
    console.log("writing ", id, "...");
    let ob = read_from_path({
      file_path: `${app_root}/backup/leaderboard/${id}.json`,
    });
    await zed_db.db
      .collection("leaderboard")
      .updateOne({ id }, { $set: ob }, { upsert: true });
  }

  // let dists = [1600];
  // for (let dist of dists) {
  //   let id = `leaderboard-${dist}`;
  //   let file_path = `${app_root}/test-1/leader-${dist}.json`;
  //   console.log("writing ", id, "...");
  //   let ob = read_from_path({ file_path });
  //   console.log(ob?.leaderboard?.length);
  //   await zed_db.db
  //     .collection("leaderboard")
  //     .updateOne({ id }, { $set: ob }, { upsert: true });
  // }
};

const test2 = async () => {
  await init();
  const zed_db = mongoose.connection;
  let dist = 1600;
  let ob = read_from_path({
    file_path: `${app_root}/test-1/leader-${dist}.json`,
  });
  let ar = ob?.leaderboard;
  let pad = (n, l = 3, d = 3) => {
    let pp = new Array(l - parseInt(n).toString().length).fill(0).join("");
    n = parseFloat(n).toFixed(d);
    return `${pp}${n}`;
  };
  ar = _.sortBy(ar, (i) => {
    return i.horse_rating.cf + pad(i.horse_rating.med);
  });
  ar = ar.map(({ hid, name, horse_rating }, i) => ({
    hid,
    rank: i + 1,
    name,
    ...horse_rating,
  }));
  // ar = ar.slice(0, 3);

  let hids = _.map(ar, "hid");
  for (let i in hids) {
    if (!(await had_last_race_in_days({ hid: hids[i], dist }))) hids[i] = null;
  }
  hids = _.compact(hids);

  let meds = hids.map((hid) => {
    let { cf, med } = _.find(ar, { hid });
    return [hid, cf + pad(med)];
  });
  meds = Object.fromEntries(meds);

  console.table(ar);
  console.log(hids);

  // let hids = [
  //   1892, 325, 463, 13089, 296, 18904, 10119, 7759, 441, 7711, 5535, 26122,
  //   7090, 980, 1978, 13135, 13613, 12315, 26113, 24920, 10918, 10656, 8607,
  //   12995, 2009,
  // ];
  // hids = await compare_heads(hids, dist);
  // let ar2 = new Array(hids.length).fill({});
  // console.log(hids);
  // for (let i in ar2) {
  // let hid = hids[i];
  // console.log(i, hid);
  // let rank = parseInt(i) + 1;
  // let { cf, d, med, name } = _.find(ar, { hid });
  // ar2[i] = { hid, rank, name, cf, d, med };
  // }
  // console.table(ar2);
  let bests = [];
  let i = 0;
  let j = 1;
  while (true && hids?.length != 0) {
    let h = hids[i++];
    console.log("RUN #", j++, "comparing", h, "against", hids.length, "horses");
    let { left, best } = await best_in_battlefield({ h, hids, meds, dist });
    console.log("in:  ", hids);
    console.log("bests:", bests, "\n=>");
    console.log("left:", left);
    console.log("best:", best);
    hids = left;
    if (best !== null) {
      bests.push(best);
      i = 0;
    }
    console.log("----------");
  }
  console.log(bests);
  hids = bests;
  let ar2 = new Array(hids.length).fill({});
  console.log(hids);
  for (let i in ar2) {
    let hid = hids[i];
    console.log(i, hid);
    let rank = parseInt(i) + 1;
    let { cf, d, med, name } = _.find(ar, { hid });
    ar2[i] = {
      hid,
      rank,
      name,
      med: cf + pad(med),
      horse_rating: { cf, d, med },
    };
  }
  console.table(ar2);
  let db_date = new Date().toISOString();
  let id = `leaderboard-${dist}`;
  let data = { id, db_date, leaderboard: ar2 };
  write_to_path({
    file_path: `${app_root}/test-2/leader-${dist}.json`,
    data,
  });
};

const test3 = async () => {
  await init();
  let h1 = 1892;
  let h2 = 10656;
  let dist = 1600;
  console.log(await had_last_race_in_days({ hid: h1, dist }));
  console.log(await had_last_race_in_days({ hid: h2, dist }));
  // check_h2_better_h1(h1, h2, dist);
};

const test4 = async () => {
  console.log("test4");
  await init();
  let ob = await zed_db.db.collection("rating_blood").findOne({});
  console.log(ob);
  let ob2 = await zed_ch.db.collection("zed").findOne({});
  console.log(ob2);
  zed_db.close();
  zed_ch.close();
};

const test5 = async () => {
  await init();
  let dists = [1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600];
  // let dists = [1600];
  for (let dist of dists) {
    console.log("Dist:", dist);
    let ob = read_from_path({
      file_path: `${app_root}/backup/leaderboard/leaderboard-${dist}.json`,
    });
    let ar = ob?.leaderboard;
    let pad = (n, l = 3, d = 3) => {
      let pp = new Array(l - parseInt(n).toString().length).fill(0).join("");
      n = parseFloat(n).toFixed(d);
      return `${pp}${n}`;
    };
    ar = _.sortBy(ar, (i) => {
      return i.horse_rating.cf + pad(i.horse_rating.med);
    });
    ar = ar.map(({ hid, name, horse_rating }, i) => ({
      hid,
      rank: i + 1,
      name,
      ...horse_rating,
    }));
    // ar = ar.slice(0, 3);

    let hids = _.map(ar, "hid");
    for (let i in hids) {
      if (!(await had_last_race_in_days({ hid: hids[i], dist })))
        hids[i] = null;
    }
    hids = _.compact(hids);

    let meds = hids.map((hid) => {
      let { cf, med } = _.find(ar, { hid });
      return [hid, cf + pad(med)];
    });
    meds = Object.fromEntries(meds);

    console.table(ar);
    console.log(hids);

    hids = await compare_heads(hids, dist);
    let ar2 = new Array(hids.length).fill({});
    for (let i in ar2) {
      let hid = hids[i];
      console.log(i, hid);
      let rank = parseInt(i) + 1;
      let { cf, d, med, name } = _.find(ar, { hid });
      ar2[i] = {
        hid,
        rank,
        name,
        med: cf + pad(med),
        horse_rating: { cf, d, med },
      };
    }
    console.table(ar2);
    let db_date = new Date().toISOString();
    let id = `leaderboard-${dist}`;
    let data = { id, db_date, leaderboard: ar2 };
    write_to_path({
      file_path: `${app_root}/test-2/leader-${dist}.json`,
      data,
    });
    await zed_db.db
      .collection("leaderboard")
      .updateOne({ id }, { $set: data }, { upsert: true });
    console.log("wrote leaderboard", dist);
  }
};

const test6 = async () => {
  await init();
  let ar = read_from_path({
    file_path: `${app_root}/required_jsons/z_medians_raw.json`,
  });
  console.log(ar.length);
  let data = [];
  for (let row of ar) {
    let [z, blood, breed, med] = row;
    z = z.toString().toLowerCase();
    blood = blood.toString().toLowerCase();
    breed = breed.toString().toLowerCase();
    let id = `${z}-${blood}-${breed}`;
    med = parseFloat(med || 0) || 0;
    let doc = { id, med };
    data.push(doc);
  }

  let cs = 10;
  for (let chunk of _.chunk(data, cs)) {
    console.log(chunk[0].id);
    await Promise.all(
      chunk.map((doc) => {
        let id = doc.id;
        zed_db.db
          .collection("z_meds")
          .updateOne({ id }, { $set: doc }, { upsert: true });
      })
    );
  }

  write_to_path({
    file_path: `${app_root}/required_jsons/z_medians_final.json`,
    data,
  });

  await zed_db.db
    .collection("z_meds")
    .updateOne(
      { id: "z_ALL" },
      { $set: { id: "z_ALL", ar: data } },
      { upsert: true }
    );

  console.log("wrote", "zed_ALL");
  zed_db.close();
};

const backend_base = "https://bs-zed-backend-api.herokuapp.com";
const get_breed_rating_hid = async (hid) => {
  if (hid == null) return null;
  let api = `${backend_base}/odds/kids-g-odds/horse/${hid}?mode=str`;
  try {
    return await fetch(api).then((r) => r.json());
  } catch (err) {
    return await get_breed_rating_hid(hid);
  }
};
const get_parents_hids = async (hid) => {
  if (hid == null) return null;
  let doc = await zed_db.db.collection("rating_blood").findOne({ hid });
  return doc?.details?.parents || { father: null, mother: null };
};
const get_parents_breed_ratings = async (hid) => {
  if (hid == null) return null;
  let parents = await get_parents_hids(hid);
  let { mother, father } = parents;
  console.log(hid, mother, father);
  let mother_br = mother && (await get_breed_rating_hid(mother));
  let father_br = father && (await get_breed_rating_hid(father));
  if (mother)
    mother_br =
      (mother_br &&
        mother_br?.gz_med &&
        parseFloat(parseFloat(mother_br?.gz_med).toFixed(3))) ||
      null;
  else mother = null;
  if (father)
    father_br =
      (father_br &&
        father_br?.gz_med &&
        parseFloat(parseFloat(father_br?.gz_med).toFixed(3))) ||
      null;
  else father = null;
  return {
    mother_hid: mother,
    mother_br: mother_br,
    father_hid: father,
    father_br,
  };
};
const test7 = async () => {
  let races_n = 150;
  await init();
  let ar = await zed_ch.db
    .collection("zed")
    .find({ 5: 0 })
    .sort({ 2: -1 })
    .limit(12 * races_n)
    .toArray();
  ar = _.groupBy(ar, "4");
  // console.log(ar);
  ar = _.entries(ar).map(([rid, ea]) => {
    let mini = _.minBy(ea, "11");
    return {
      rid,
      hid: mini["6"],
      name: mini["9"],
      odds: mini["11"],
      date: mini["2"],
    };
  });
  ar = ar.filter((e) => e.odds != 0);
  let hids = _.map(ar, "hid");
  let br_ob = await Promise.all(
    hids.map((hid) => get_parents_breed_ratings(hid).then((d) => [hid, d]))
  );
  br_ob = Object.fromEntries(br_ob);
  ar = ar.map((ea, i) => {
    let hid = ea.hid;
    let br = br_ob[hid] || {
      mother_hid: null,
      mother_br: null,
      father_hid: null,
      father_br: null,
    };
    return { ...ea, ...br };
  });
  ar = ar.filter((ea) => {
    let { father_br, mother_br } = ea;
    if (father_br == mother_br) return false;
    return true;
  });
  console.table(ar);
};

const test8 = async () => {
  await init();
  await download_eth_prices();
};

module.exports = {
  test1,
  test2,
  test3,
  test4,
  test5,
  test6,
  test7,
};
