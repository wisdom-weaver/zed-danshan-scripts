const mongoose = require("mongoose");
const mdb = require("./connection/mongo_connect");
const v3 = require("./v3/v3");
const mod = v3;

const main = async (args) => {
  await mdb.init();
  console.log("main");
  let [_node, _cfile, arg1, arg2, arg3, arg4] = args;
  if (arg1 == "--rating_flames") {
    if (arg2 == "all") mod.rating_flames.all();
    if (arg2 == "only") {
      let conf = JSON.parse(arg3) || {};
      mod.rating_flames.only(conf);
    }
    if (arg2 == "range") {
      let [a, b] = [parseInt(arg3), parseInt(arg4)];
      mod.rating_flames.range(a, b);
    }
  } else if (arg1 == "--rating_blood") {
    if (arg2 == "all") mod.rating_blood.all();
    if (arg2 == "only") {
      let conf = JSON.parse(arg3) || {};
      mod.rating_blood.only(conf);
    }
    if (arg2 == "range") {
      let [a, b] = [parseInt(arg3), parseInt(arg4)];
      mod.rating_blood.range(a, b);
    }
    if (arg2 == "generate_ranks") {
      mod.rating_blood.generate_ranks();
    }
    if (arg2 == "test") {
      mod.rating_blood.test(arg3);
    }
  } else if (arg1 == "--base_ability") {
    if (arg2 == "all") mod.base_ability.all();
    if (arg2 == "only") {
      let conf = JSON.parse(arg3) || {};
      mod.base_ability.only(conf);
    }
    if (arg2 == "range") {
      let [a, b] = [parseInt(arg3), parseInt(arg4)];
      mod.base_ability.range(a, b);
    }
    if (arg2 == "generate_table") {
      mod.base_ability.generate_table();
    }
    if (arg2 == "test") {
      mod.base_ability.test(arg3);
    }
  }
};
main(process.argv);
