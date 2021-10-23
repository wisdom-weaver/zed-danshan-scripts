const _ = require("lodash");
const fetch = require("node-fetch");
const mongoose = require("mongoose");
const { init, zed_db, zed_ch, run_func } = require("./index-run");
const { write_to_path, read_from_path } = require("./utils");
const app_root = require("app-root-path");
const {
  get_ed_horse,
  add_new_horse_from_zed_in_bulk,
} = require("./zed_horse_scrape");
const { delay } = require("./utils");
const {
  update_odds_and_breed_for_race_horses,
} = require("./odds-generator-for-blood2");

const z = (geno) => {
  if (!geno) return null;
  if (geno?.startsWith("Z")) geno = geno.slice(1);
  return parseInt(geno);
};

const validate_kid_parents = (doc) => {
  if (_.isEmpty(doc)) return null;
  const { hid, genotype, parents, parents_d } = doc;
  // console.log(parents);
  if (_.chain(parents).values().compact().isEmpty().value()) return null;
  let { mother, father } = parents;
  if (_.isEmpty(parents_d)) return false;
  let { mother: m_d, father: f_d } = parents_d || {};
  let k_z = z(genotype);
  let m_z = z(m_d.genotype);
  let f_z = z(f_d.genotype);
  // console.log({ k_z, m_z, f_z });
  if (!k_z || !m_z || !f_z) return false;
  return m_z + f_z == k_z;
};

const validate_kids_n_parents = async () => {
  await init();
  console.log("validate_kids_n_parents");
  let cs = 500;
  let id = "invalid_kids";
  let ed_hid = await get_ed_horse();
  let [st, ed] = [1, ed_hid];
  // let h = 128220;
  // let [st, ed] = [h, ed_hid];
  console.log("getting", st, "->", ed);
  let hids = new Array(ed - st + 1).fill(0).map((e, i) => i + st);
  for (let chunk_hids of _.chunk(hids, cs)) {
    // console.log(chunk_hids);
    let [st_h, ed_h] = [chunk_hids[0], chunk_hids[chunk_hids.length - 1]];
    let docs = await zed_db
      .collection("horse_details")
      .find(
        { hid: { $in: chunk_hids } },
        {
          projection: { _id: 0, hid: 1, genotype: 1, parents: 1, parents_d: 1 },
        }
      )
      .toArray();
    let this_eval = _.chain(docs)
      .map((d) => d?.hid && [d.hid, validate_kid_parents(d)])
      .compact()
      .fromPairs()
      .value();
    let invalid_kids = _.chain(this_eval)
      .entries()
      .filter((i) => i[1] === false)
      .map((i) => parseInt(i[0]))
      .compact()
      .value();
    // console.log(invalid_kids);
    console.log(st_h, "->", ed_h, "found", invalid_kids.length, "invalid kids");
    await zed_db.db
      .collection("script")
      .updateOne(
        { id },
        { $set: { id }, $addToSet: { invalid_kids: { $each: invalid_kids } } },
        { upsert: true }
      );
    await delay(2000);
  }
};

const remove_kid = async (hid) => {
  let kid_hid = hid;
  let doc = await zed_db
    .collection("horse_details")
    .find({ hid }, { _id: 0, hid: 1, parents: 1 });
  if (_.isEmpty(doc)) return;
  let { parents } = doc;
  if (_.chain(parents).values().compact().isEmpty().value()) return;
  let { mother, father } = parents;
  let bulk = [mother, father].map((hid) => ({
    updateOne: {
      filter: { hid },
      update: { $pullAll: { offsprings: [kid_hid] } },
    },
  }));
  await zed_db.db.collection("horse_details").bulkWrite(bulk);
};

const update_invalid_kids_n_parents = async () => {
  await init();
  let cs = 10;
  let id = "invalid_kids";
  let { invalid_kids = [] } = await zed_db.db
    .collection("script")
    .findOne({ id });
  // invalid_kids = [128220];
  console.log("invalid_kids", invalid_kids.length);
  for (let chunk of _.chunk(invalid_kids, cs)) {
    console.log(chunk);
    await chunk.map(remove_kid);
    await add_new_horse_from_zed_in_bulk(chunk);
    let ob = _.chain(chunk)
      .map((i) => [i, null])
      .fromPairs()
      .value();
    await update_odds_and_breed_for_race_horses(ob);
    await zed_db.db.collection("script").updateOne(
      { id },
      {
        $pullAll: { invalid_kids: chunk },
      }
    );
  }
};

const fix_parents_kids_mismatch = async () => {
  await validate_kids_n_parents();
  await update_invalid_kids_n_parents();
  console.log("done");
};

module.exports = {
  fix_parents_kids_mismatch,
};
