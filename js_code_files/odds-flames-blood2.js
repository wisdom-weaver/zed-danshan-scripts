const _ = require("lodash");
const { struct_race_row_data, initiate } = require("./cyclic_dependency");
const { get_at_eth_price_on } = require("./base");
const { zed_db, zed_ch, init } = require("./index-run");

const classes = ["#", 0, 1, 2, 3, 4, 5];
const fees = ["#", "A", "B", "C", "F"];
const dists = ["####", 1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600];
const keys = (() => {
  let ar = [];
  for (let c of classes)
    for (let f of fees) for (let d of dists) ar.push(`${c}${f}${d}`);
  return ar;
})();

const fee_tags_ob = {
  A: [25.0, 22.5, 5000],
  B: [15.0, 15.0, 22.5],
  C: [10.0, 7.5, 15],
  D: [5.0, 3.75, 7.5],
  E: [2.5, 1.25, 3.75],
  F: [0.0, 0.0, 0.0],
};
const get_fee_tag = (entryfee_usd) => {
  for (let [tag, [rep, mi, mx]] of _.entries(fee_tags_ob))
    if (_.inRange(entryfee_usd, mi, mx + 1e-3)) return tag;
};

const filter_acc_to_criteria = ({
  races = [],
  criteria = {},
  extra_criteria = {},
}) => {
  // races = races.filter((ea) => ea.odds != 0);
  if (_.isEmpty(criteria)) return races;

  let fr = _.filter(races, (v) => {
    let b = true;
    if (criteria?.is_paid === true && parseFloat(v.entryfee) == 0) return false;
    if (criteria.c !== "#") b &= criteria.c == v.thisclass;
    if (criteria.f !== "#") b &= criteria.f == v.fee_tag;
    if (criteria.d !== "####") b &= criteria.d == v.distance;
    return b;
  });
  if (criteria?.min !== undefined && fr?.length < criteria.min) return [];
  return fr;
};

const f_crit_min = 3;
const generate_odds_flames = async ({ hid, races = [] }) => {
  try {
    races = races.map((r) => {
      let entryfee_usd = parseFloat(r.entryfee) * get_at_eth_price_on(r.date);
      return { ...r, entryfee_usd };
    });
    races = races.map((r) => {
      return { ...r, fee_tag: get_fee_tag(r.entryfee_usd) };
    });
    let ob = {};
    for (let k of keys) {
      let c = k[0];
      let f = k[1];
      let d = k.slice(2);
      if (c !== "#") c = parseInt(c);
      if (d !== "####") d = parseInt(d);
      let fr = filter_acc_to_criteria({
        races,
        criteria: { c, f, d, is_paid: true },
      });
      let flames_n = _.filter(fr, { flame: 1 })?.length || 0;
      let len = fr.length || 0;
      let flames_per = flames_n < f_crit_min ? null : (flames_n * 100) / len;
      // if (k == "#B2200") {
      // console.table(fr);
      // console.log({ len, flames_n, flames_per });
      // }
      // ob[k] = { len, flames_n, flames_per };
      ob[k] = flames_per;
    }
    return { hid, races_n: races.length, odds_flames: ob };
  } catch (err) {
    console.log("ERROR odds_flames", hid);
    return null;
  }
};
const generate_odds_flames_hid = async (hid) => {
  let races = await zed_ch.db.collection("zed").find({ 6: hid }).toArray();
  races = struct_race_row_data(races);
  let ob = await generate_odds_flames({ hid, races });
  // console.log(ob);
  console.log("#", hid, "len:", races.length, "fl_all%", ob?.odds_flames["######"]);
  return ob;
};

const runner = async () => {
  await initiate();
  let hid = 3312;
  let odds_flames = await generate_odds_flames_hid(hid);
  console.log(odds_flames);
  // await zed_db.db
  //   .collection("rating_breed2")
  //   .updateOne({ hid: 2202 }, { $set: { br: 1 } }, { upsert: true });
  console.log("done");
};
// runner();

module.exports = {
  generate_odds_flames,
  generate_odds_flames_hid,
};
