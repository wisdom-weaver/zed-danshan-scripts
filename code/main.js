const mongoose = require("mongoose");
const mdb = require("./connection/mongo_connect");
const dan = require("./dan/dan");
const global_req = require("./global_req/global_req");
const race_horses = require("./races/race_horses");
const zed_races = require("./races/zed_races");
const tests = require("./tests/tests");
const utils = require("./utils/utils");
const tourneyr01 = require("./v3/tourneyr01");
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
    if (arg2 == "test") {
      let conf = JSON.parse(arg3) || {};
      mod.rating_flames.test(conf);
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
      let conf = JSON.parse(arg3) || {};
      mod.rating_blood.test(conf);
    }
  } else if (arg1 == "--ancestry") {
    if (arg2 == "all") mod.ancestry.all();
    if (arg2 == "only") {
      let conf = JSON.parse(arg3) || {};
      mod.ancestry.only(conf);
    }
    if (arg2 == "range") {
      let [a, b] = [parseInt(arg3), parseInt(arg4)];
      mod.ancestry.range(a, b);
    }
    if (arg2 == "generate_ranks") {
      mod.ancestry.generate_ranks();
    }
    if (arg2 == "test") {
      let conf = JSON.parse(arg3) || {};
      mod.ancestry.test(conf);
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
    if (arg2 == "all") mod.rating_breed.all();
    if (arg2 == "only") {
      let conf = JSON.parse(arg3) || {};
      mod.rating_breed.only(conf);
    }
    if (arg2 == "range") {
      let [a, b] = [parseInt(arg3), parseInt(arg4)];
      mod.rating_breed.range(a, b);
    }
  } else if (arg1 == "--ymca2_table") {
    if (arg2 == "generate") mod.ymca2_table.generate();
    if (arg2 == "get") {
      let ob = mod.ymca2_table.get(1);
      console.table(ob);
    }
    if (arg2 == "test") {
      let ob = mod.ymca2_table.test(1);
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
    if (arg2 == "only_w_parents") {
      let cs = arg4 ? parseInt(arg4) : def_cs;
      let conf = JSON.parse(arg3) || {};
      mod.mega.only_w_parents(conf, cs);
    }
    if (arg2 == "only_w_parents_br") {
      let cs = arg4 ? parseInt(arg4) : def_cs;
      let conf = JSON.parse(arg3) || {};
      mod.mega.only_w_parents_br(conf, cs);
    }
    if (arg2 == "range") {
      let cs = arg5 ? parseInt(arg5) : def_cs;
      console.log({ cs });
      let [a, b] = [parseInt(arg3), parseInt(arg4)];
      mod.mega.range(a, b, cs);
    }
    if (arg2 == "range_w_parents_br") {
      let cs = arg5 ? parseInt(arg5) : def_cs;
      console.log({ cs });
      let [a, b] = [utils.get_n(arg3), utils.get_n(arg4)];
      mod.mega.range_w_parents_br(a, b, cs);
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
  } else if (arg1 == "--horses") {
    if (arg2 == "new") {
      mod.horses.get_new();
    }
    if (arg2 == "range") {
      arg3 = JSON.parse(arg3) ?? [0, 0];
      mod.horses.get_range(arg3);
    }
    if (arg2 == "only") {
      arg3 = JSON.parse(arg3) ?? [0];
      mod.horses.get_only(arg3);
    }
    if (arg2 == "missing") {
      arg3 = JSON.parse(arg3) ?? [0];
      mod.horses.get_missings(arg3);
    }
    if (arg2 == "new_hdocs") {
      mod.horses.get_new_hdocs();
    }
    if (arg2 == "range_hdocs") {
      arg3 = JSON.parse(arg3) ?? [0, 0];
      mod.horses.get_range_hdocs(arg3);
    }
    if (arg2 == "only_hdocs") {
      arg3 = JSON.parse(arg3) ?? [0];
      mod.horses.get_only_hdocs(arg3);
    }
    if (arg2 == "fix_unnamed") {
      mod.horses.fix_unnamed();
    }
    if (arg2 == "fix_unnamed_cron") {
      mod.horses.fix_unnamed_cron();
    }
    if (arg2 == "fix_stable") {
      mod.horses.fix_stable();
    }
    if (arg2 == "fix_stable_cron") {
      mod.horses.fix_stable_cron();
    }
  } else if (arg1 == "--parents") {
    if (arg2 == "fix_horse_type_all_cron") {
      mod.parents.fix_horse_type_all_cron();
    }
    if (arg2 == "fix_horse_type_all") {
      mod.parents.fix_horse_type_all();
    }
  } else if (arg1 == "--dan_max_gap") {
    if (arg2 == "main") {
      dan.max_gap.main(arg3, arg4);
    }
    if (arg2 == "test") {
      dan.max_gap.test();
    }
  } else if (arg1 == "--ranks") {
    if (arg2 == "run") mod.ranks.run();
    if (arg2 == "run_cron") mod.ranks.run_cron();
  } else if (arg1 == "--tourneyr01") {
    if (arg2 == "test") {
      tourneyr01.test();
    }
    if (arg2 == "now") {
      tourneyr01.now();
    }
    if (arg2 == "run_cron") {
      tourneyr01.run_cron();
    }
    if (arg2 == "now_h") {
      tourneyr01.now_h();
    }
    if (arg2 == "run_cron_h") {
      tourneyr01.run_cron_h();
    }
    if (arg2 == "run_dur") {
      tourneyr01.run_dur(arg3);
    }
  } else if (arg1 == "--tests") {
    try {
      arg3 = JSON.parse(arg3);
    } catch (err) {
      arg3 = null;
    }
    if (arg2 == "run") tests.run(arg3);
  }
  console.log("---ed");
};
main(process.argv);
