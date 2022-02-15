const { get_races_of_hid } = require("../utils/cyclic_dependency");
const mdb = require("../connection/mongo_connect");
const _ = require("lodash");
const bulk = require("../utils/bulk");
const { zed_db } = require("../connection/mongo_connect");
const cyclic_depedency = require("../utils/cyclic_dependency");
const coll = "hraces_stats";
const name = "hraces_stats v3";
const cs = 500;
let test_mode = 0;

const get_races_stats = async ({ hid, races }) => {
  let rids = _.map(races, "raceid");
  // console.log(rids);
  let profits = await Promise.all(
    rids.map((race_id) =>
      cyclic_depedency.get_prize({ hid, race_id }).then((d) => {
        if (_.isEmpty(d)) return 0;
        let { prize = 0, fee = 0 } = d;
        let profit = parseFloat(prize) - parseFloat(fee);
        if (!profit || _.isNaN(profit)) profit = 0;
        return profit;
      })
    )
  );
  // console.log(profits);
  let races_n = races.length;
  let win_rate =
    (_.filter(races, (i) => parseInt(i.place) == 1)?.length ?? 0) /
    (races_n || 1);
  let flame_rate =
    (_.filter(races, (i) => parseInt(i.flame) == 1)?.length ?? 0) /
    (races_n || 1);
  let profit = _.chain(profits).sum().value();
  return { races_n, win_rate, flame_rate, profit };
};

const calc = async ({ hid, races = [], tc }) => {
  try {
    hid = parseInt(hid);
    races = _.uniqBy(races, (i) => `${i.raceid}${i.hid}`);
    // console.log(races.length)
    let free = _.filter(races, (i) => i.fee_tag == "F" && i.thisclass !== 99);
    let paid = _.filter(races, (i) => i.fee_tag !== "F" && i.thisclass !== 99);
    let tourney = _.filter(races, (i) => i.thisclass == 99);
    // console.log("free",free.length)
    // console.log("paid",paid.length)
    // console.log("tourney",tourney.length)
    let ob = { free: free, paid: paid, tourney: tourney };
    for (let [k, rs] of _.entries(ob)) {
      let v = await get_races_stats({ hid, races: rs });
      ob[k] = v;
    }
    ob.hid = hid;
    return ob;
  } catch (err) {
    console.log("err on get_rating_flames", hid);
    console.log(err);
  }
};
const generate = async (hid) => {
  hid = parseInt(hid);
  let races = await get_races_of_hid(hid);
  let doc =
    (await zed_db.db.collection("horse_details").findOne({ hid }, { tc: 1 })) ||
    null;
  if (doc == null) return null;
  let tc = doc?.tc;
  let ob = await calc({ races, tc, hid });
  // console.log(ob);
  return ob;
};
const all = async () => bulk.run_bulk_all(name, generate, coll, cs, test_mode);
const only = async (hids) =>
  bulk.run_bulk_only(name, generate, coll, hids, cs, test_mode);
const range = async (st, ed) =>
  bulk.run_bulk_range(name, generate, coll, st, ed, cs, test_mode);

const test = async (hids) => {
  for (let hid of hids) {
    try {
      test_mode = 1;
      let ob = await generate(hid);
      console.log(hid, ob);
      // let o = await cyclic_depedency.get_prize({race_id: "0jznIhpd",hid: 586,});
      // console.log(o);
    } catch (err) {
      console.log(err);
    }
  }
};

const hraces_stats = {
  test,
  calc,
  generate,
  all,
  only,
  range,
};
module.exports = hraces_stats;
