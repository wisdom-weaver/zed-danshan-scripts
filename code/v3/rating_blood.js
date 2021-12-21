const { get_races_of_hid } = require("../utils/cyclic_dependency");
const mdb = require("../connection/mongo_connect");
const _ = require("lodash");
const bulk = require("../utils/bulk");
const { zed_db } = require("../connection/mongo_connect");
const { calc_race_score } = require("./race_score");
const coll = "rating_blood3";
const name = "rating_blood v3";
const cs = 200;
const test_mode = 0;

const calc_roi = (r) => {
  let fee = _.reduce(r, (n, i) => n + (i?.entryfee_usd || 0), 0);
  let profit =
    _.sum(
      [
        [1, 7.19],
        [2, 2.39],
        [3, 1.19],
      ].map(([p, ratio]) => {
        let prof_p = r.map((i) => {
          let { place, entryfee_usd } = i;
          if (place == p) return entryfee_usd * ratio;
          else return 0;
        });
        return _.sum(prof_p);
      })
    ) ?? 0;
  return ((profit - fee) / (fee || 1)) * 100;
};
const def_rating_blood = {
  tunnel: null,
  rat: null,
  win_rate: null,
  roi: null,
};
const calc = async ({ hid, races = [], tc }) => {
  try {
    hid = parseInt(hid);
    if (races?.length == 0) return { tunnel, rat: null };
    // if (print) console.log(races[0]);
    let ob = ["S", "M", "D"].map((tunnel) => {
      let filt_races = _.filter(races, { tunnel });
      let r_ob = filt_races.map((r) => {
        let { thisclass: rc, fee_tag, place: position, flame } = r;
        let score = calc_race_score({ rc, fee_tag, position, flame });
        let final_score = score * 0.1;
        return {
          rc,
          fee_tag,
          position,
          flame,
          score,
          final_score,
        };
      });
      let rat = _.meanBy(r_ob, "final_score") ?? null;
      return { rat, tunnel };
    });
    let rat_ob = _.maxBy(ob, "rat");
    if (rat_ob?.rat == null) return { ...def_rating_blood, hid, tc };
    let filt_races = _.filter(races, { tunnel: rat_ob.tunnel });
    let win_rate = _.filter(filt_races, (i) =>
      ["1"].includes(i.place.toString())
    );
    win_rate = ((win_rate?.length || 0) / (filt_races?.length || 1)) * 100;
    let roi = calc_roi(filt_races);
    return { hid, tc, ...rat_ob, win_rate, roi };
  } catch (err) {
    console.log("err on get_rating_flames", hid);
    console.log(err);
  }
};
const generate = async (hid) => {
  hid = parseInt(hid);
  let races = await get_races_of_hid(hid);
  let doc = await zed_db.db
    .collection("horse_details")
    .findOne({ hid }, { tc: 1 });
  let tc = doc?.tc || undefined;
  let ob = await calc({ races, tc, hid });
  if (test_mode) console.log(hid, ob);
  return ob;
};
const all = async () => bulk.run_bulk_all(name, generate, coll, cs, test_mode);
const only = async (hids) =>
  bulk.run_bulk_only(name, generate, coll, hids, cs, test_mode);
const range = async (st, ed) =>
  bulk.run_bulk_range(name, generate, coll, st, ed, cs, test_mode);

const test = async () => {
  let ob = await generate(3312);
  console.log(ob);
};

const rating_blood = {
  test,
  calc,
  generate,
  all,
  only,
  range,
};
module.exports = rating_blood;
