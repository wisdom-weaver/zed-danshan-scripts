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
const cron = require("node-cron");
const cron_parser = require("cron-parser");

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
  return Math.min(m_z + f_z, 268) == k_z;
};

const validate_kids_n_parents = async () => {
  let id = "invalid_kids";
  await init();
  await zed_db.db
    .collection("script")
    .updateOne({ id }, { $set: { id, invalid_kids: [] } }, { upsert: true });
  console.log("validate_kids_n_parents");
  let cs = 500;
  let ed_hid = await get_ed_horse();
  let [st, ed] = [1, ed_hid];
  // let h = 130000;
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
    console.log(invalid_kids);
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
  await fix_horse_type_all();
  console.log("done");
};

const fix_horse_type_after_kids = async (hids_) => {
  // await init();
  let docs_a = await zed_db.db
    .collection("rating_breed2")
    .find(
      {
        hid: { $in: hids_ },
      },
      { projection: { _id: 0, kids_n: 1, hid: 1 } }
    )
    .limit(10)
    .toArray();
  // console.table(docs_a);
  let hids = _.map(docs_a, "hid");
  let docs_b =
    (await zed_db.db
      .collection("horse_details")
      .find(
        { hid: { $in: hids } },
        { projection: { _id: 0, hid: 1, horse_type: 1 } }
      )
      .toArray()) || [];
  // console.table(docs_b);
  docs_a = _.chain(docs_a).keyBy("hid").mapValues("kids_n").value();
  // console.log(docs_a);
  // console.log(docs_b);
  let bulk = docs_b.map((doc) => {
    let { hid = null, horse_type = null } = doc || {};
    if (!hid || !horse_type) return null;
    let kids_n = docs_a[hid];
    let n_horse_type = null;
    // ["Stallion", "Colt", "Mare", "Filly"],
    if (["Filly", "Mare"].includes(horse_type))
      n_horse_type = kids_n >= 1 ? "Mare" : "Filly";
    if (["Colt", "Stallion"].includes(horse_type))
      n_horse_type = kids_n >= 1 ? "Stallion" : "Colt";
    if (!n_horse_type || horse_type === n_horse_type) return null;
    console.log(hid, n_horse_type);
    return {
      updateOne: {
        filter: { hid },
        update: { $set: { horse_type: n_horse_type } },
      },
    };
  });
  bulk = _.compact(bulk);
  if (!_.isEmpty(bulk))
    await zed_db.db.collection("horse_details").bulkWrite(bulk);
  console.log(
    hids_[0],
    "->",
    hids_[hids_.length - 1],
    `update horse_type`,
    bulk.length,
    "horses"
  );
};
const fix_horse_type_using_kid_ids = async (kids) => {
  let docs = await zed_db.db
    .collection("horse_details")
    .find(
      { hid: { $in: kids } },
      { projection: { _id: 0, hid: 1, parents: 1 } }
    )
    .toArray();
  let parents = docs.map((e) => {
    let { mother, father } = e?.parents || {};
    return [mother, father];
  });
  parents = _.chain(parents).flatten().compact().value();
  // console.log(parents);
  await fix_horse_type_after_kids(parents);
};

const fix_horse_type_all = async () => {
  await init();
  let cs = 100;
  let ed_hid = await get_ed_horse();
  let [st, ed] = [1, ed_hid];
  // let h = 130000;
  // let [st, ed] = [h, ed_hid];
  console.log("getting", st, "->", ed);
  let hids = new Array(ed - st + 1).fill(0).map((e, i) => i + st);
  for (let chunk_hids of _.chunk(hids, cs)) {
    await fix_horse_type_after_kids(chunk_hids);
  }
};

const fix_horse_type_all_cron = async () => {
  let cron_str = "0 0 */4 * * *";
  console.log(
    "#starting fix_horse_type_all_cron",
    cron_str,
    cron.validate(cron_str)
  );
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("Next run:", c_itvl.next().toISOString());

  const runner = () => {
    console.log("#running leaderboard_b2_cron");
    const c_itvl = cron_parser.parseExpression(cron_str);
    console.log("Next run:", c_itvl.next().toISOString());
    generate_leaderboard_b2();
  };
  cron.schedule(cron_str, runner);
};

const runner = async () => {
  await init();
  // fix_horse_type_after_kids([92597]);
  // fix_horse_type_all();
  // fix_horse_type_using_kid_ids([124088]);
};
// runner();

module.exports = {
  fix_parents_kids_mismatch,
  fix_horse_type_all,
  fix_horse_type_all_cron,
  fix_horse_type_using_kid_ids,
};
