const _ = require("lodash");

const rating_flames = require("./rating_flames");
const rating_blood = require("./rating_blood");
const rating_breed = require("./rating_breed");
const base_ability = require("./base_ability");
const ymca2 = require("./ymca2");
const ymca2_table = require("./ymca2_table");
const hraces_stats = require("./hraces_stats");

const v5_ymca5 = require("../v5/ymca5");
const v5_rating_breed = require("../v5/rating_breed");

const { zed_ch, zed_db } = require("../connection/mongo_connect");
const {
  get_races_of_hid,
  get_ed_horse,
} = require("../utils/cyclic_dependency");
const bulk = require("../utils/bulk");
const { get_hids } = require("../utils/utils");
const utils = require("../utils/utils");
const est_ymca = require("./est_ymca");
const dp = require("./dp");
const zedf = require("../utils/zedf");
const cyclic_depedency = require("../utils/cyclic_dependency");

const s3 = {
  rating_flames,
  rating_blood,
  rating_breed,
  base_ability,
  ymca2,
  ymca2_table,
  hraces_stats,
  est_ymca,
  dp,
};

const s5 = {
  ymca5: v5_ymca5,
  rating_breed: v5_rating_breed,
};

let test_mode = 0;
const def_cs = 25;

const calc = async ({ hid }) => {
  hid = parseInt(hid);
  let hdoc = await zed_db.db
    .collection("horse_details")
    .findOne({ hid }, { tc: 1 });
  if (_.isEmpty(hdoc)) {
    console.log("empty horse", hid);
    hdoc = await zedf
      .horse(hid)
      .then((doc) => cyclic_depedency.struct_zed_hdoc(hid, doc));
    // console.log(hdoc);
    if (!hdoc) return null;
    await zed_db.db
      .collection("horse_details")
      .updateOne({ hid }, { $set: hdoc }, { upsert: true });
    // return;
  }
  let tc = hdoc?.tc || null;
  let races = await get_races_of_hid(hid);
  if (test_mode) {
    console.log("#hid", hid, "class:", tc, "races_n:", races.length);
  }
  let [
    rating_blood,
    rating_breed,
    rating_flames,
    base_ability,
    ymca2,
    est_ymca,
    dp4,
    hraces_stats,

    ymca5,
    breed5,
  ] = await Promise.all([
    s3.rating_blood.calc({ hid, races, tc }),
    s3.rating_breed.calc({ hid, tc }),
    s3.rating_flames.calc({ hid, races, tc }),
    s3.base_ability.calc({ hid, races, tc, hdoc }),
    s3.ymca2.calc({ hid, races, details: hdoc, from: "mega" }),
    s3.est_ymca.calc({ hid, races, hdoc }),
    s3.dp.calc({ hid, races, hdoc }),
    s3.hraces_stats.calc({ hid, races, hdoc }),

    s5.ymca5.calc({ hid, races, hdoc, from: "mega" }),
    s5.rating_breed.calc({ hid, races, hdoc }),
  ]);
  if (test_mode) {
    console.log("rating_blood", rating_blood);
    console.log("rating_breed", rating_breed);
    console.log("rating_flames", rating_flames);
    console.log("base_ability", base_ability);
    console.log("ymca2", ymca2);
    console.log("dp4", dp4);
    console.log("hraces_stats", hraces_stats);
  }
  let ymca2_doc = { hid, ymca2 };
  let est_ymca_doc = { hid, est_ymca };

  let ymca5_doc = { hid, ymca5 };

  return {
    hid,
    rating_blood,
    rating_breed,
    rating_flames,
    base_ability,
    ymca2_doc,
    est_ymca_doc,
    dp4,
    hraces_stats,
    ymca5_doc,
    breed5,
  };
};

const generate = async (hid) => {
  let docs = await calc({ hid });
  return docs;
};

const push_mega_bulk = async (datas_ar) => {
  let rating_blood_bulk = [];
  let rating_breed_bulk = [];
  let rating_flames_bulk = [];
  let base_ability_bulk = [];
  let ymca2_doc_bulk = [];
  let est_ymca_doc_bulk = [];
  let dp4_bulk = [];
  let hraces_stats_bulk = [];

  let ymca5_doc_bulk = [];
  let breed5_bulk = [];

  datas_ar = _.compact(datas_ar);
  datas_ar.map((data) => {
    let {
      hid,
      rating_blood,
      rating_breed,
      rating_flames,
      base_ability,
      ymca2_doc,
      est_ymca_doc,
      dp4,
      hraces_stats,

      ymca5_doc,
      breed5,
    } = data;
    if (!_.isEmpty(rating_blood)) rating_blood_bulk.push(rating_blood);
    if (!_.isEmpty(rating_breed)) rating_breed_bulk.push(rating_breed);
    if (!_.isEmpty(rating_flames)) rating_flames_bulk.push(rating_flames);
    if (!_.isEmpty(base_ability)) base_ability_bulk.push(base_ability);
    if (!_.isEmpty(ymca2_doc)) ymca2_doc_bulk.push(ymca2_doc);
    if (!_.isEmpty(est_ymca_doc)) est_ymca_doc_bulk.push(est_ymca_doc);
    if (!_.isEmpty(dp4)) dp4_bulk.push(dp4);
    if (!_.isEmpty(hraces_stats)) hraces_stats_bulk.push(hraces_stats);
    if (!_.isEmpty(ymca5_doc)) ymca5_doc_bulk.push(ymca5_doc);
    if (!_.isEmpty(breed5)) breed5_bulk.push(breed5);
  });

  if (test_mode) {
    console.log("rating_blood_bulk.len", rating_blood_bulk.length);
    console.log("rating_breed_bulk.len", rating_breed_bulk.length);
    console.log("rating_flames_bulk.len", rating_flames_bulk.length);
    console.log("base_ability_bulk.len", base_ability_bulk.length);
    console.log("ymca2_doc_bulk.len", ymca2_doc_bulk.length);
    console.log("est_ymca_doc_bulk.len", est_ymca_doc_bulk.length);
    console.log("dp4_bulk.len", dp4_bulk.length);
    console.log("hraces_stats_bulk.len", hraces_stats_bulk.length);
  }
  await Promise.all([
    bulk.push_bulk("rating_blood3", rating_blood_bulk, "rating_blood"),
    bulk.push_bulk("rating_breed3", rating_breed_bulk, "rating_breed"),
    bulk.push_bulk("rating_flames3", rating_flames_bulk, "rating_flames"),
    bulk.push_bulk("rating_blood3", base_ability_bulk, "base_ability"),
    bulk.push_bulk("rating_breed3", ymca2_doc_bulk, "ymca2"),
    bulk.push_bulk("rating_breed3", est_ymca_doc_bulk, "est_ymca"),
    bulk.push_bulk("dp4", dp4_bulk, "dp4"),
    bulk.push_bulk("hraces_stats", hraces_stats_bulk, "hraces_stats"),
    bulk.push_bulk("ymca5", ymca5_doc_bulk, "ymca5"),
    bulk.push_bulk("rating_breed5", breed5_bulk, "breed5"),
  ]);
  let [a, b] = [datas_ar[0]?.hid, datas_ar[datas_ar.length - 1]?.hid];
  console.log("pushed_mega_bulk", datas_ar.length, `[${a} -> ${b}]`);
};

const only_w_parents = async (hids, cs = def_cs) => {
  for (let chunk_hids of _.chunk(hids, cs)) {
    let datas_ar = await Promise.all(chunk_hids.map((hid) => generate(hid)));
    datas_ar = _.compact(datas_ar);
    await push_mega_bulk(datas_ar);
    await utils.delay(500);
    let phids = await zed_db.db
      .collection("horse_details")
      .find({ hid: { $in: chunk_hids } }, { projection: { parents: 1 } })
      .toArray();
    phids = _.chain(phids)
      .map((i) => _.values(i.parents))
      .flatten()
      .compact()
      .value();
    let datas_ar_p = await Promise.all(phids.map((hid) => generate(hid)));
    datas_ar_p = _.compact(datas_ar_p);
    await push_mega_bulk(datas_ar_p);
  }
};
const only_w_parents_br = async (hids, cs = def_cs) => {
  for (let chunk_hids of _.chunk(hids, cs)) {
    let datas_ar = await Promise.all(chunk_hids.map((hid) => generate(hid)));
    datas_ar = _.compact(datas_ar);
    await push_mega_bulk(datas_ar);
    await utils.delay(500);
    let phids = await zed_db.db
      .collection("horse_details")
      .find({ hid: { $in: chunk_hids } }, { projection: { parents: 1 } })
      .toArray();
    phids = _.chain(phids)
      .map((i) => _.values(i.parents))
      .flatten()
      .compact()
      .value();
    await s3.rating_breed.only(phids);
    await s5.rating_breed.only(phids);
  }
};

const only = async (hids, cs = def_cs) => {
  for (let chunk_hids of _.chunk(hids, cs)) {
    let datas_ar = await Promise.all(chunk_hids.map((hid) => generate(hid)));
    datas_ar = _.compact(datas_ar);
    await push_mega_bulk(datas_ar);
  }
};

const range = async (st, ed, cs = def_cs) => {
  if (!ed || ed == "ed") ed = await get_ed_horse();
  let hids = get_hids(st, ed);
  await only(hids, cs);
  console.log("ended", name);
};
const range_w_parents_br = async (st, ed, cs = def_cs) => {
  if (!ed || ed == "ed") ed = await get_ed_horse();
  let hids = get_hids(st, ed);
  await only_w_parents_br(hids, cs);
  console.log("ended", name);
};

const all = async (cs = def_cs) => {
  let [st, ed] = [1, await get_ed_horse()];
  let hids = get_hids(st, ed);
  await only(hids);
};

const test = async (hids) => {
  test_mode = 1;
  for (let hid of hids) {
    let ob = await generate(hid);
    console.log(ob);
  }
};

const mega = {
  calc,
  generate,
  test,
  only,
  range,
  all,
  only_w_parents,
  only_w_parents_br,
  range_w_parents_br,
};
module.exports = mega;
