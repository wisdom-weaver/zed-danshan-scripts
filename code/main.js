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
  }
};
main(process.argv);
