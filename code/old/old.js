const _ = require("lodash");
// const { generate_odds_flames_hid } = require("./odds-flames-blood2");
const generate_odds_flames = () => null;

const filter_acc_to_criteria = ({
  races = [],
  criteria = {},
  extra_criteria = {},
}) => {
  // races = races.filter((ea) => ea.odds != 0);
  if (_.isEmpty(criteria)) return races;
  races = races.filter(
    ({
      distance,
      date,
      entryfee,
      raceid,
      thisclass,
      hid,
      finishtime,
      place,
      name,
      gate,
      odds,
      fee_cat,
    }) => {
      entryfee = parseFloat(entryfee);
      if (odds == 0 || odds == null) return false;
      if (criteria?.thisclass !== undefined && criteria?.thisclass !== "#")
        if (!(thisclass == criteria.thisclass)) return false;

      if (criteria?.fee_cat !== undefined && criteria?.fee_cat !== "#")
        if (!(fee_cat == criteria.fee_cat)) return false;

      if (criteria?.distance !== undefined && criteria?.distance !== "####")
        if (!(distance == criteria.distance)) return false;

      if (criteria?.is_paid !== undefined)
        if (criteria?.is_paid == true && entryfee == 0) return false;

      return true;
    }
  );
  if (criteria?.min !== undefined) {
    if (races?.length < criteria.min) {
      // console.log("less", min);
      return [];
    }
  }
  return races;
};

const calc_median = (array = []) => {
  if (_.isEmpty(array)) return null;
  array = array.map(parseFloat).sort((a, b) => a - b);
  let median = 0;
  if (array.length == 0) return null;
  if (array.length % 2 === 0) {
    // array with even number elements
    median = (array[array.length / 2] + array[array.length / 2 - 1]) / 2;
  } else {
    median = array[(array.length - 1) / 2]; // array with odd number elements
  }
  // console.log(array, median)
  return median;
};

const cls = ["#", 0, 1, 2, 3, 4, 5];
const dists = ["####", 1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600];
const fee_tags = ["#", "A", "B", "C", "D", "E", "F"];

const get_keys_map = ({ cls, dists, fee_tags }) => {
  let odds_modes = [];
  for (let c of cls)
    for (let f of fee_tags)
      for (let d of dists) odds_modes.push(`${c}${f}${d}`);
  odds_modes = _.uniq(odds_modes);
  return odds_modes;
};
const get_odds_map = ({
  cls,
  dists,
  fee_tags,
  races = [],
  extra_criteria = {},
}) => {
  let odds_modes = get_keys_map({ cls, dists, fee_tags });
  let ob = {};
  odds_modes.forEach((o) => {
    let c = o[0];
    let f = o[1];
    let d = o.slice(2);

    let fr = filter_acc_to_criteria({
      races,
      criteria: { thisclass: c, fee_cat: f, distance: d, ...extra_criteria },
    });
    let odds_ar = Object.values(_.mapValues(fr, "odds"));
    ob[o] = odds_ar;
  });
  return ob;
};

const gen_odds_coll = ({ coll, races = [], extra_criteria = {} }) => {
  let odds_map = get_odds_map({ cls, dists, fee_tags, races, extra_criteria });
  let ob = {};
  // console.log(odds_map);
  for (let key in odds_map) {
    ob[key] = calc_median(odds_map[key]);
  }
  return ob;
};

const or_map = [
  { coll: "odds_overall", extra_criteria: {} },
  { coll: "odds_live", extra_criteria: { is_paid: true, min: 3 } },
];

const old = {
  or_map,
  gen_odds_coll,
  gen_odds_coll,
  generate_odds_flames,
};

module.exports = old;
