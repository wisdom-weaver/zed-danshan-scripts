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
  general_bulk_push,
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
  let cron_str = "0 0 */8 * * *";
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

const get_global_br_avg_hid = async (hid) => {
  if (hid == null) return null;
  hid = parseInt(hid);
  let { bloodline, breed_type, genotype } = await zed_db.db
    .collection("horse_details")
    .findOne({ hid }, { _id: 0, bloodline: 1, breed_type: 1, genotype: 1 });
  let key = `${bloodline}-${breed_type}-${genotype}`;
  let doc_id = "kid-score-global";
  let doc = await zed_db.db
    .collection("requirements")
    .findOne({ id: doc_id }, { [`avg_ob.${key}`]: 1 });
  return doc?.avg_ob[key]?.br_avg || null;
};

const get_parents_comb_score = async (hid) => {
  let { parents } = await zed_db.db
    .collection("horse_details")
    .findOne({ hid }, { parents: 1 });
  let { mother, father } = parents;
  // console.log(mother, father);
  if (!mother && !father) return null;
  let { br: f_br = 0 } = await zed_db.db
    .collection("rating_breed2")
    .findOne({ hid: father }, { br: 1 });

  let { br: m_br = 0 } = await zed_db.db
    .collection("rating_breed2")
    .findOne({ hid: mother }, { br: 1 });

  let comb_score = _.sum([f_br, m_br]);
  // console.log(comb_score);
  return comb_score;
};

const parents_comb_score_generator_all_horses = async (st, ed, cs=50) => {
  try {
    await init();
    // await init_btbtz();
    if (!st) st = 1;
    else st = parseFloat(st);
    if (!ed) ed = await get_ed_horse();
    else ed = parseFloat(ed);
    // let cs = 50;
    let chunk_delay = 100;
    let hids = new Array(ed - st + 1).fill(0).map((ea, idx) => st + idx);
    // let hids = [26646, 21744, 21512];
    // let hids = [21888];
    // outer: while (true) {
    console.log("=> STARTED parents_comb_score_generator: ", `${st}:${ed}`);
    for (let chunk of _.chunk(hids, cs)) {
      // console.log("\n=> fetching together:", chunk.toString());
      let obar = await Promise.all(
        chunk.map((hid) =>
          get_parents_comb_score(hid).then((comb_score) => ({
            hid,
            comb_score,
          }))
        )
      );
      console.log(obar);
      obar = _.compact(obar);
      if (obar.length == 0) {
        // console.log("starting from initial");
        // continue outer;
        console.log("exit");
        return;
      }
      // console.table(obar);
      try {
        await general_bulk_push("rating_breed2", obar);
      } catch (err) {
        console.log(err);
        console.log("mongo err");
      }
      console.log("! got", chunk[0], " -> ", chunk[chunk.length - 1]);
      await delay(chunk_delay);
    }
    // }
  } catch (err) {
    console.log("ERROR fetch_all_horses\n", err);
  }
};

const runner = async () => {
  await init();
  await parents_comb_score_generator_all_horses(31896, 31896);
  // console.log(await get_parents_comb_score(31896));
  // fix_horse_type_after_kids([92597]);
  // fix_horse_type_all();
  // fix_horse_type_using_kid_ids([124088]);
};
// runner();

module.exports = {
  parents_comb_score_generator_all_horses,
  fix_parents_kids_mismatch,
  fix_horse_type_all,
  fix_horse_type_all_cron,
  fix_horse_type_using_kid_ids,
};
