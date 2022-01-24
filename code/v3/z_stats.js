const { zed_ch, zed_db } = require("../connection/mongo_connect");
const utils = require("../utils/utils");
const _ = require("lodash");
const bulk = require("../utils/bulk");
const { get_all_hids } = require("../utils/cyclic_dependency");

const [z_mi, z_mx] = [1, 268];
const dists = [1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600];
const places = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
const cs = 100;
const g_race_adder_only = async (hids) => {
  hids = _.map(hids, (i) => parseInt(i));
  let geno_docs = await zed_db.db
    .collection("horse_details")
    .find(
      { hid: { $in: hids } },
      { projection: { hid: 1, genotype: 1, _id: 0 } }
    )
    .toArray();
  geno_docs = _.chain(geno_docs)
    .map((i) => [i.hid, i.genotype])
    .fromPairs()
    .value();
  // console.log(geno_docs);
  let races_docs = await zed_ch.db
    .collection("zed")
    .find(
      { 5: 0, 6: { $in: hids } },
      { projection: { _id: 0, 4: 1, 6: 1, 8: 1, 13: 1, 1: 1 } }
    )
    .toArray();
  races_docs = races_docs.map((doc) => {
    let { 4: rid, 1: dist, 8: place, 13: flame, 6: hid } = doc;
    let genotype = geno_docs[hid];
    return {
      rid,
      hid,
      genotype,
      dist: parseInt(dist),
      place: parseInt(place),
      flame: parseInt(flame),
    };
  });
  // console.table(races_docs);
  await bulk.push_bulk("g_races", races_docs, "g_races");
};
const g_race_adder_all = async () => {
  let hids = await get_all_hids();
  for (let chunk_hids of _.chunk(hids, cs)) {
    await g_race_adder_only(chunk_hids);
  }
  console.log("end");
};

const get_zg_hids = async ({ genotype, dist, place, flame }) => {
  dist = parseInt(dist);
  place = parseInt(place);
  flame = parseInt(flame);
  let races = await zed_db.db
    .collection("g_races")
    .find(
      {
        dist,
        place,
        flame,
        genotype,
      },
      { projection: { _id: 0, hid: 1 } }
    )
    .toArray();
  let zg_hids = _.map(races, "hid");
  return zg_hids;
};

const get_z_stats = async (inp) => {
  let { genotype, flame, dist, place } = inp;
  flame = parseInt(flame);
  dist = parseInt(dist);
  place = parseInt(place);

  let z = utils.geno(genotype);
  let zg_hids = [];
  for (let i = 0; i < z_mx; i++) {
    let nz;
    if (i !== 0) {
      if (i % 2 == 0) nz = z - Math.ceil((1 * i) / 2);
      else nz = z + Math.ceil((1 * i) / 2);
    } else nz = z;
    nz = Math.max(z_mi - 1, nz);
    nz = Math.min(z_mx + 1, nz);
    if (nz == z_mi - 1 || nz == z_mx + 1) continue;

    let genotype = `Z${nz}`;
    let nzg_hids = await get_zg_hids({ genotype, dist, place, flame });
    // let nzg_hids = [];
    // console.log(genotype, nzg_hids.length);
    zg_hids = [...zg_hids, ...nzg_hids];
    zg_hids = _.uniq(zg_hids);
    if (zg_hids.length > 100) break;
  }

  let zg_hids_n = zg_hids.length;
  // console.log({ zg_hids_n, zg_hids });
  let dist_ob = {};
  for (let d of dists) {
    let draces = await zed_ch.db
      .collection("zed")
      .find(
        {
          1: { $in: [parseInt(d), d.toString()] },
          6: { $in: zg_hids },
        },
        { projection: { _id: 0, 8: 1 } }
      )
      .toArray();
    let n = draces.length;
    let wins = draces.filter((i) => parseInt(i[8]) == 1)?.length || 0;
    let win_rate = wins / (n || 1);
    const dist_o = { n, wins, win_rate };
    dist_ob[d] = win_rate;
  }
  // console.table(dist_ob);
  return { genotype, flame, dist, place, zg_hids_n, dist_ob };
  return dist_ob;
};

const generate = async () => {
  for (let z = z_mi; z <= z_mx; z++)
    for (let place of places)
      for (let dist of dists)
        for (let flame of [0, 1]) {
          let genotype = `Z${z}`;
          console.log(genotype, { place, dist, flame });
          let ob = await get_z_stats({ genotype, place, dist, flame });
          await zed_db.db
            .collection("z_stats")
            .updateOne(
              { genotype, place, dist, flame },
              { $set: ob },
              { upsert: true }
            );
        }
  console.log("Ended");
};

const run = async () => {
  let inp = {
    genotype: "Z7",
    flame: 1,
    dist: 1000,
    place: 9,
  };
  let ob = await get_z_stats(inp);
  console.log(ob);
};

const test = async () => {};

const z_stats = { test, run, g_race_adder_all, generate };
module.exports = z_stats;
