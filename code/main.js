const mongoose = require("mongoose");
const mdb = require("./connection/mongo_connect");
const global_req = require("./global_req/global_req");
const race_horses = require("./races/race_horses");
const zed_races = require("./races/zed_races");
const v3 = require("./v3/v3");
const mod = v3;

const main = async (args) => {
  await mdb.init();
  await global_req.download();
  console.log("main");
  let [_node, _cfile, arg1, arg2, arg3, arg4, arg5] = args;
  if (arg1 == "--races") {
    if (arg2 == "test") zed_races.test();
    if (arg2 == "live") zed_races.live();
    if (arg2 == "live_cron") zed_races.live_cron();
    if (arg2 == "miss") zed_races.miss(arg3, arg4);
    if (arg2 == "miss_cron") zed_races.miss_cron();
  } else if (arg1 == "--rating_flames") {
    if (arg2 == "all") mod.rating_flames.all();
    if (arg2 == "only") {
      let conf = JSON.parse(arg3) || {};
      mod.rating_flames.only(conf);
    }
    if (arg2 == "range") {
      let [a, b] = [parseInt(arg3), parseInt(arg4)];
      mod.rating_flames.range(a, b);
    }
  } else if (arg1 == "--race_horses") {
    if (arg2 == "test") {
      race_horses.test();
    }
    if (arg2 == "run_cron") {
      race_horses.run_cron();
    }
  } else if (arg1 == "--rating_flames") {
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
      let conf = JSON.parse(arg3) || {};
      mod.base_ability.test(conf);
    }
  } else if (arg1 == "--ymca2") {
    if (arg2 == "all") mod.ymca2.all();
    if (arg2 == "only") {
      let conf = JSON.parse(arg3) || {};
      mod.ymca2.only(conf);
    }
    if (arg2 == "range") {
      let [a, b] = [parseInt(arg3), parseInt(arg4)];
      mod.ymca2.range(a, b);
    }
    if (arg2 == "test") {
      let conf = JSON.parse(arg3) || {};
      mod.ymca2.test(conf);
    }
  } else if (arg1 == "--rating_breed") {
    if (arg2 == "test") {
      let conf = JSON.parse(arg3) || {};
      mod.rating_breed.test(conf);
    }
  } else if (arg1 == "--ymca2_table") {
    if (arg2 == "generate") mod.ymca2_table.generate();
    if (arg2 == "get") {
      let ob = mod.ymca2_table.get(1);
      console.table(ob);
    }
  } else if (arg1 == "--mega") {
    let def_cs = 25;
    if (arg2 == "all") {
      let cs = arg3 ? parseInt(arg3) : def_cs;
      mod.mega.all(cs);
    }
    if (arg2 == "only") {
      let cs = arg4 ? parseInt(arg4) : def_cs;
      let conf = JSON.parse(arg3) || {};
      mod.mega.only(conf, cs);
    }
    if (arg2 == "range") {
      let cs = arg5 ? parseInt(arg5) : def_cs;
      let [a, b] = [parseInt(arg3), parseInt(arg4)];
      mod.mega.range(a, b, cs);
    }
    if (arg2 == "test") {
      let conf = JSON.parse(arg3) || {};
      mod.mega.test(conf);
    }
  } else if (arg1 == "--parents_comb") {
    if (arg2 == "all") {
      mod.parents_comb.all();
    }
    if (arg2 == "only") {
      let conf = JSON.parse(arg3) || {};
      mod.parents_comb.only(conf);
    }
    if (arg2 == "range") {
      let [a, b] = [parseInt(arg3), parseInt(arg4)];
      mod.parents_comb.range(a, b);
    }
    if (arg2 == "test") {
      let conf = JSON.parse(arg3) || {};
      mod.parents_comb.test(conf);
    }
  }
};
main(process.argv);
