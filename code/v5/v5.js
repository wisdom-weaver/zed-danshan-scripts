const { jparse } = require("../utils/cyclic_dependency");
const line = require("./line");
const rating_breed = require("./rating_breed");
const rcount = require("./rcount");
const ymca5_s = require("./ymca5");
const ymca5_table = require("./ymca5_table");

const mod = {
  ymca5: ymca5_s,
  ymca5_table,
  rating_breed,
  rcount,
  line,
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
    if (arg2 == "cron") {
      const runner = () => mod.ymca5_table.generate();
      const cron_str = `0 0 * * 1,4`;
      cyclic_depedency.print_cron_details(cron_str);
      cron.schedule(cron_str, async () => {
        console.log("started", iso());
        await runner();
        console.log("ended", iso());
      });
    }
    if (arg2 == "get") {
      let ob = await mod.ymca5_table.get(1);
      console.table(ob);
    }
    if (arg2 == "test") {
      let ob = await mod.ymca5_table.test(1);
      console.table(ob);
    }
  } else if (arg1 == "--breed") {
    if (arg2 == "test") {
      let conf = JSON.parse(arg3) || {};
      await mod.rating_breed.test(conf);
    }
    if (arg2 == "all") await mod.rating_breed.all();
    if (arg2 == "only") {
      let conf = JSON.parse(arg3) || {};
      await mod.rating_breed.only(conf);
    }
    if (arg2 == "range") {
      await mod.rating_breed.range(jparse(arg3));
    }
    if (arg2 == "parents_of") {
      await mod.rating_breed.parents_of(jparse(arg3));
    }
    if (arg2 == "fixer") {
      await mod.rating_breed.fixer();
    }
  } else if (arg1 == "--rcount") {
    console.log("# rcount");
    if (arg2 == "all") await mod.rcount.all();
    if (arg2 == "only") {
      let conf = JSON.parse(arg3) || {};
      await mod.rcount.only(conf);
    }
    if (arg2 == "range") {
      await mod.rcount.range(jparse(arg3));
    }
    if (arg2 == "fixer") {
      await mod.rcount.fixer();
    }
    if (arg2 == "test") {
      let conf = JSON.parse(arg3) || {};
      await mod.rcount.test(conf);
    }
    if (arg2 == "fix") {
      await mod.rcount.fix();
    }
  } else if (arg1 == "--line") {
    console.log("# line");
    if (arg2 == "all") await mod.line.all();
    if (arg2 == "only") {
      let conf = JSON.parse(arg3) || {};
      await mod.line.only(conf);
    }
    if (arg2 == "range") {
      await mod.line.range(jparse(arg3));
    }
    if (arg2 == "fixer") {
      await mod.line.fixer();
    }
    if (arg2 == "test") {
      let conf = JSON.parse(arg3) || {};
      await mod.line.test(conf);
    }
    if (arg2 == "pair_test") {
      let conf = JSON.parse(arg3) || {};
      await mod.line.pair_test(conf);
    }
    if (arg2 == "fix") {
      await mod.line.fix();
    }
    if (arg2 == "run_cron") {
      await mod.line.run_cron();
    }
  }
};

const v5 = {
  ...mod,
  main_runner,
};
module.exports = v5;
