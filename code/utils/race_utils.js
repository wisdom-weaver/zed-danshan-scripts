const _ = require("lodash");

const cf_keys = (() => {
  let keys = [];
  for (let c of [1, 2, 3, 4, 5])
    for (let f of ["A", "B", "C", "D", "E"]) keys.push(`${c}${f}`);
  return keys;
})();

const str = (n) => n.toString();

const filter_races = (races, query, conf = {}) => {
  let ar = [
    ["c", "thisclass"],
    ["f", "fee_tag"],
    ["t", "tunnel"],
    ["d", "distance"],
    ["flame", "flame"],
    ["position", "place"],
  ];
  if (conf?.no_tourney == 1) {
    races = _.filter(races, (i) => i.thisclass != 99);
  }
  if (conf?.paid == 1) {
    races = _.filter(races, (i) => parseFloat(i.entryfee) != 0);
  }
  let filt = _.filter(races, (races) => {
    return ar.reduce((acc, [k, r_key]) => {
      if (acc == false) return false;
      if (_.isArray(query[k]) && _.isEmpty(query[k])) return true;
      if (query[k] === null || query[k] === undefined) return true;
      if (_.isArray(query[k]))
        return query[k].map(str).includes(str(races[r_key]));
      else return str(query[k]) == str(races[r_key]);
    }, true);
  });
  return filt;
};

const race_utils = {
  filter_races,
  cf_keys,
};
module.exports = race_utils;
