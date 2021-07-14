const _ = require("lodash");

const calc_avg = (ar = []) => {
  ar = _.compact(ar);
  if (_.isEmpty(ar)) return null;
  let sum = ar.reduce((acc, ea) => acc + ea, 0);
  return sum / ar.length;
};

module.exports = {
  calc_avg,
};
