const { generate_blood_mapping } = require("./index-odds-generator");
const {
  compare_heads,
  best_in_battlefield,
} = require("./leaderboard-generator");
const { init } = require("./index-run");
const _ = require("lodash");
const mongoose = require("mongoose");
const { write_to_path, read_from_path } = require("./utils");
const app_root = require("app-root-path");

const test1 = async () => {
  await init();
  // let ob = [
  //   { a: null, b: 1 },
  //   { a: 2, b: 3 },
  //   { a: null, b: 2 },
  //   { a: null, b: 4 },
  // ];
  let zed_db = mongoose.connection;
  // let datas = await zed_db.db.collection("leaderboard").find({}).toArray();
  // for (let data of datas) {
  //   console.log("writing ", data.id, "...");
  //   write_to_path({
  //     file_path: `${app_root}/backup/leaderboard/${data.id}.json`,
  //     data,
  //   });
  // }

  // let dists = ["All", 1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600];
  // for (let dist of dists) {
  //   let id = `leaderboard-${dist}`;
  //   console.log("writing ", id, "...");
  //   let ob = read_from_path({
  //     file_path: `${app_root}/backup/leaderboard/${id}.json`,
  //   });
  //   await zed_db.db
  //     .collection("leaderboard")
  //     .updateOne({ id }, { $set: ob }, { upsert: true });
  // }

  let dists = [1600];
  for (let dist of dists) {
    let id = `leaderboard-${dist}`;
    let file_path = `${app_root}/test-1/leader-${dist}.json`;
    console.log("writing ", id, "...");
    let ob = read_from_path({ file_path });
    console.log(ob?.leaderboard?.length);
    await zed_db.db
      .collection("leaderboard")
      .updateOne({ id }, { $set: ob }, { upsert: true });
  }
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

module.exports = {
  test1,
  test2,
};
