const { zed_ch, zed_db } = require("../connection/mongo_connect");
const utils = require("../utils/utils");
const _ = require("lodash");
const bulk = require("../utils/bulk");
const { get_all_hids } = require("../utils/cyclic_dependency");

const dists = [1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600];
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
const run = async () => {
  g_race_adder_all();
};

const test = async () => {};

const z_stats = { test, run, g_race_adder_all };
module.exports = z_stats;
