const _ = require("lodash");

const rating_flames = require("./rating_flames");
const rating_blood = require("./rating_blood");
const rating_breed = require("./rating_breed");
const base_ability = require("./base_ability");
const ymca2 = require("./ymca2");
const ymca2_table = require("./ymca2_table");

const { zed_ch, zed_db } = require("../connection/mongo_connect");
const { get_races_of_hid } = require("../utils/cyclic_dependency");

const s = {
  rating_flames,
  rating_blood,
  rating_breed,
  base_ability,
  ymca2,
  ymca2_table,
};

let test_mode = 0;
const cs = 100;

const calc = async ({ hid }) => {};
const generate = async (hid) => {
  console.log("mega", hid);
  let doc = await zed_db.db.collection("horse_details").findOne({ hid });
  let races = await get_races_of_hid(hid);
  // let [rating_blood, rating_breed, rating_flames, base_ability] =
  //   await Promise.all([
  //     v3.rating_breed.calc({ hid }),
  //     v3.rating_flames.calc({ hid, races }),
  //     v3.base_ability.calc({ hid, races }),
  //   ]);
  let rating_blood = await s.rating_blood.calc({ hid, races, tc: doc.tc });
  console.log("rating_blood", rating_blood);
  // console.log("rating_breed", rating_breed);
  // console.log("rating_flames", rating_flames);
  // console.log("base_ability", base_ability);
};

// const all = async () => bulk.run_bulk_all(name, generate, coll, cs, test_mode);
// const only = async (hids) =>
// bulk.run_bulk_only(name, generate, coll, hids, cs, test_mode);
// const range = async (st, ed) =>
// bulk.run_bulk_range(name, generate, coll, st, ed, cs, test_mode);

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
  // all,
  // only,
  // range,
};
module.exports = mega;
