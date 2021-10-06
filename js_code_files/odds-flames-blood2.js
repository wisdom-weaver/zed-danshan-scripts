const { initiate } = require("./odds-generator-for-blood2");
const { struct_race_row_data } = require("./cyclic_dependency");
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
  return races;
};

const odds_flames_generator = async ({ hid, races = [] }) => {
  races = struct_race_row_data(races);
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
    let fr = filter_acc_to_criteria({ races, criteria: { c, f, d } });
    ob[k] = fr.length;
  }
  return ob;
};

const runner = async () => {
  await initiate();
  let hid = 3312;
  let races = await zed_ch.db.collection("zed").find({ 6: hid }).toArray();
  let odds_flames = await odds_flames_generator({ hid, races });
  console.table(odds_flames);
};
// runner();