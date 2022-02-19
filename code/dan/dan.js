const compiler_ba = require("./compiler/compiler_ba");
const compiler_dp = require("./compiler/compiler_dp");
const compiler_rng = require("./compiler/compiler_rng");
const max_gap = require("./max_gap");

const dan = {
  max_gap,
  compiler_dp,
  compiler_rng,
  compiler_ba,
};
module.exports = dan;
