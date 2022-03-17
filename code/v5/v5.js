const { jparse } = require("../utils/cyclic_dependency");
const ymca5_s = require("./ymca5");
const ymca5_table = require("./ymca5_table");
const mod = {
  ymca5: ymca5_s,
  ymca5_table,
};

const main_runner = async (args) => {
  let [_node, _cfile, v, arg1, arg2, arg3, arg4, arg5] = args;
  console.log("# v5");
  if (arg1 == "--ymca5") {
    console.log("# ymca5");
    if (arg2 == "all") await mod.ymca5.all();
    if (arg2 == "only") {
      let conf = JSON.parse(arg3) || {};
      await mod.ymca5.only(conf);
    }
    if (arg2 == "range") {
      await mod.ymca5.range(jparse(arg3));
    }
    if (arg2 == "fixer") {
      await mod.ymca5.fixer();
    }
    if (arg2 == "test") {
      let conf = JSON.parse(arg3) || {};
      await mod.ymca5.test(conf);
    }
  } else if (arg1 == "--ymca5_table") {
    if (arg2 == "generate") await mod.ymca5_table.generate();
    if (arg2 == "get") {
      let ob = await mod.ymca5_table.get(1);
      console.table(ob);
    }
    if (arg2 == "test") {
      let ob = await mod.ymca5_table.test(1);
      console.table(ob);
    }
  }
};

const v5 = {
  ...mod,
  main_runner,
};
module.exports = v5;
