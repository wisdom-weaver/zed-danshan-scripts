const _ = require("lodash");

const rating_flames = require("./rating_flames");
const rating_blood = require("./rating_blood");
const rating_breed = require("./rating_breed");
const base_ability = require("./base_ability");
const ymca2 = require("./ymca2");
const ymca2_table = require("./ymca2_table");

const { zed_ch, zed_db } = require("../connection/mongo_connect");
const { get_races_of_hid } = require("../utils/cyclic_dependency");
const bulk = require("../utils/bulk");
const { get_hids } = require("../utils/utils");

const s_ = {
  rating_flames,
  rating_blood,
  rating_breed,
  base_ability,
  ymca2,
  ymca2_table,
};

let test_mode = 1;
const cs = 3;

const calc = async ({ hid }) => {
  hid = parseInt(hid);
  let hdoc = await zed_db.db
    .collection("horse_details")
    .findOne({ hid }, { tc: 1 });
  if (!_.isEmpty(hdoc)) {
    console.log("empty horse", hid);
    return null;
  }
  let tc = hdoc?.tc || null;
  let races = await get_races_of_hid(hid);
  if (test_mode) {
    console.log("#hid", hid, "class:", tc, "races_n:", races.length);
  }
  let [rating_blood, rating_breed, rating_flames, base_ability] =
    await Promise.all([
      s_.rating_blood.calc({ hid, races, tc }),
      s_.rating_breed.calc({ hid, tc }),
      s_.rating_flames.calc({ hid, races, tc }),
      s_.base_ability.calc({ hid, races, tc }),
    ]);
  if (test_mode) {
    console.log("rating_blood", rating_blood);
    console.log("rating_breed", rating_breed);
    console.log("rating_flames", rating_flames);
    console.log("base_ability", base_ability);
  }
  return { hid, rating_blood, rating_breed, rating_flames, base_ability };
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
  datas_ar = _.compact(datas_ar);
  datas_ar.map((data) => {
    let { hid, rating_blood, rating_breed, rating_flames, base_ability } = data;
    if (!_.isEmpty(rating_blood)) rating_blood_bulk.push(rating_blood);
    if (!_.isEmpty(rating_breed)) rating_breed_bulk.push(rating_breed);
    if (!_.isEmpty(rating_flames)) rating_flames_bulk.push(rating_flames);
    if (!_.isEmpty(base_ability)) base_ability_bulk.push(base_ability);
  });
  if (test_mode) {
    console.log("rating_blood_bulk.len", rating_blood_bulk.length);
    console.log("rating_breed_bulk.len", rating_breed_bulk.length);
    console.log("rating_flames_bulk.len", rating_flames_bulk.length);
    console.log("base_ability_bulk.len", base_ability_bulk.length);
  }
  await Promise.all([
    bulk.push_bulk("rating_blood3", rating_blood_bulk),
    bulk.push_bulk("rating_breed3", rating_breed_bulk),
    bulk.push_bulk("rating_flames3", rating_flames_bulk),
    bulk.push_bulk("rating_blood3", base_ability_bulk),
  ]);
  let [a, b] = [datas_ar[0]?.hid, datas_ar[datas_ar.length]?.hid];
  console.log("pushed_mega_bulk", datas_ar.length, `[${a} -> ${b}]`);
};

const only = async (hids) => {
  for (let chunk_hids of _.chunk(hids, cs)) {
    let datas_ar = await Promise.all(chunk_hids.map((hid) => generate(hid)));
    datas_ar = _.compact(datas_ar);
    await push_mega_bulk(datas_ar);
  }
};

const range = async (st, ed) => {
  if (!ed || ed == "ed") ed = await get_ed_horse();
  let hids = get_hids(st, ed);
  await only(hids);
  console.log("ended", name);
};

const all = async () => {
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
};
module.exports = mega;
