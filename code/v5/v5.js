const { jparse } = require("../utils/cyclic_dependency");
const ymca5_s = require("./ymca5");

const mod = {
  ymca5: ymca5_s,
};

const main_runner = async (args) => {
  let [_node, _cfile, v, arg1, arg2, arg3, arg4, arg5] = args;
  console.log("# v5");
  if (arg1 == "--ymca5") {
    console.log("# ymca5");
    if (arg2 == "all") mod.ymca5.all();
    if (arg2 == "only") {
      let conf = JSON.parse(arg3) || {};
      mod.ymca5.only(conf);
    }
    if (arg2 == "range") {
      mod.ymca5.range(jparse(arg3));
    }
    if (arg2 == "fixer") {
      mod.ymca5.fixer();
    }
    if (arg2 == "test") {
      let conf = JSON.parse(arg3) || {};
      mod.ymca5.test(conf);
    }
  }
};

const v5 = {
  ...mod,
  main_runner,
};
module.exports = v5;
