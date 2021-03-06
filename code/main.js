const mongoose = require("mongoose");
const mdb = require("./connection/mongo_connect");
const dan = require("./dan/dan");
const global_req = require("./global_req/global_req");
const race_horses = require("./races/race_horses");
const zed_races = require("./races/zed_races");
const tests = require("./tests/tests");
const utils = require("./utils/utils");
const v3 = require("./v3/v3");
const v5 = require("./v5/v5");
const z_stats = require("./v3/z_stats");
const gap = require("./v3/gaps");
const { jparse } = require("./utils/cyclic_dependency");
const payments = require("./payments/payments");
const finder = require("./tests/finder");
const gapi = require("../gapi/gapi");
const temp = require("../temp/temp");
const tourney = require("./tourney/tourney");
const hawku = require("./hawku/hawku");
const { fixers } = require("./fixers/fixer");
const mate = require("./mate/mate");
const { race_speed_adj } = require("./races/race_speed_adj");
const base = require("./utils/base");
const helpers = require("./helpers/helper");
const transfers = require("./hawku/transfers");
const { tqual } = require("./tqual/tqual");
const stables_s = require("./stables/stables");
const sn_pro = require("./sn_pro/sn_pro");
const mod = v3;
const send_weth = require("./payments/send_weth");
const hclass = require("./hclass/hclass");

const main = async (args) => {
  await mdb.init();
  await global_req.download();
  await gapi.init();
  await base.download_eth_prices();

  console.log("main");
  let [_node, _cfile, arg1, arg2, arg3, arg4, arg5] = args;
  if (arg1 == "--races") {
    await zed_races.main_runner();
  } else if (arg1 == "--compiler_dp") {
    if (arg2 == "test") await dan.compiler_dp.test();
    if (arg2 == "run") await dan.compiler_dp.run();
    if (arg2 == "runner") await dan.compiler_dp.runner();
    if (arg2 == "run_cron") await dan.compiler_dp.run_cron();
    if (arg2 == "run_hs") {
      arg3 = jparse(arg3) ?? [];
      await dan.compiler_dp.run_hs(arg3);
    }
    if (arg2 == "run_range") {
      arg3 = jparse(arg3) ?? [];
      await dan.compiler_dp.run_range(arg3);
    }
  } else if (arg1 == "--compiler_rng") {
    if (arg2 == "test") await dan.compiler_rng.test();
    if (arg2 == "run") await dan.compiler_rng.run();
    if (arg2 == "runner") await dan.compiler_rng.runner();
    if (arg2 == "run_cron") await dan.compiler_rng.run_cron();
    if (arg2 == "run_hs") {
      arg3 = jparse(arg3) ?? [];
      await dan.compiler_rng.run_hs(arg3);
    }
    if (arg2 == "run_range") {
      arg3 = jparse(arg3) ?? [];
      await dan.compiler_rng.run_range(arg3);
    }
  } else if (arg1 == "--compiler_ba") {
    if (arg2 == "test") await dan.compiler_ba.test();
    if (arg2 == "run") await dan.compiler_ba.run();
    if (arg2 == "runner") await dan.compiler_ba.runner();
    if (arg2 == "run_cron") await dan.compiler_ba.run_cron();
    if (arg2 == "run_hs") {
      arg3 = jparse(arg3) ?? [];
      await dan.compiler_ba.run_hs(arg3);
    }
    if (arg2 == "run_range") {
      arg3 = jparse(arg3) ?? [];
      await dan.compiler_ba.run_range(arg3);
    }
  } else if (arg1 == "--gap") {
    if (arg2 == "test") await gap.test();
    if (arg2 == "fix") await gap.fix();
    if (arg2 == "run_dur") await gap.run_dur(arg3, arg4);
    if (arg2 == "manual") {
      arg3 = arg3?.split(",") ?? [];
      await gap.manual(arg3);
    }
    if (arg2 == "run_hids") {
      arg3 = jparse(arg3) ?? [];
      await gap.run_hids(arg3);
    }
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
  } else if (arg1 == "--hraces_stats") {
    if (arg2 == "all") await mod.hraces_stats.all();
    if (arg2 == "only") {
      let conf = jparse(arg3) || {};
      await mod.hraces_stats.only(conf);
    }
    if (arg2 == "test") {
      let conf = JSON.parse(arg3) || {};
      await mod.hraces_stats.test(conf);
    }
    if (arg2 == "range") {
      let [a, b] = [parseInt(arg3), parseInt(arg4)];
      await mod.hraces_stats.range(a, b);
    }
  } else if (arg1 == "--race_horses") {
    await race_horses.main_runner();
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
  } else if (arg1 == "--dp") {
    if (arg2 == "all") mod.dp.all();
    if (arg2 == "fix") await mod.dp.fix();
    if (arg2 == "only") {
      let conf = JSON.parse(arg3) || {};
      mod.dp.only(conf);
    }
    if (arg2 == "range") {
      let [a, b] = [parseInt(arg3), parseInt(arg4)];
      mod.dp.range(a, b);
    }
    if (arg2 == "test") {
      let conf = JSON.parse(arg3) || {};
      mod.dp.test(conf);
    }
  } else if (arg1 == "--rating_blood") {
    if (arg2 == "all") await mod.rating_blood.all();
    if (arg2 == "fix") await mod.rating_blood.fix();
    if (arg2 == "only") {
      let conf = JSON.parse(arg3) || {};
      await mod.rating_blood.only(conf);
    }
    if (arg2 == "range") {
      let [a, b] = [parseInt(arg3), parseInt(arg4)];
      await mod.rating_blood.range(a, b);
    }
    if (arg2 == "generate_ranks") {
      await mod.rating_blood.generate_ranks();
    }
    if (arg2 == "test") {
      let conf = JSON.parse(arg3) || {};
      await mod.rating_blood.test(conf);
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
    if (arg2 == "fixer") {
      mod.ymca2.fixer();
    }
    if (arg2 == "test") {
      let conf = JSON.parse(arg3) || {};
      mod.ymca2.test(conf);
    }
  } else if (arg1 == "--est_ymca") {
    if (arg2 == "all") mod.est_ymca.all();
    if (arg2 == "only") {
      let conf = JSON.parse(arg3) || {};
      mod.est_ymca.only(conf);
    }
    if (arg2 == "range") {
      let [a, b] = [parseInt(arg3), parseInt(arg4)];
      mod.est_ymca.range(a, b);
    }
    if (arg2 == "test") {
      let conf = JSON.parse(arg3) || {};
      mod.est_ymca.test(conf);
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
    if (arg2 == "fixer") {
      mod.rating_breed.fixer();
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
    if (arg2 == "allw") {
      let cs = arg3 ? parseInt(arg3) : def_cs;
      mod.mega.allw(cs);
    }
    if (arg2 == "allw_br") {
      let cs = arg3 ? parseInt(arg3) : def_cs;
      mod.mega.allw_br(cs);
    }
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
    await mod.horses.main_runner();
  } else if (arg1 == "--parents") {
    if (arg2 == "fix_horse_type_all_cron") {
      mod.parents.fix_horse_type_all_cron();
    }
    if (arg2 == "fix_horse_type_all") {
      mod.parents.fix_horse_type_all();
    }
    if (arg2 == "fix_parents_kids_mismatch") {
      mod.parents.fix_parents_kids_mismatch();
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
  } else if (arg1 == "--z_stats") {
    if (arg2 == "test") {
      z_stats.test();
    }
    if (arg2 == "run") {
      z_stats.run();
    }
    if (arg2 == "generate") {
      z_stats.generate();
    }
  } else if (arg1 == "--payments") {
    if (arg2 == "test") await payments.test();
    if (arg2 == "test_2") await payments.test_2();
    if (arg2 == "fix") await payments.fix();
    if (arg2 == "runner") await payments.runner();
    if (arg2 == "run_cron") await payments.run_cron();
    if (arg2 == "run_dur") {
      await payments.run_dur(arg3, arg4);
    }
  } else if (arg1 == "--tests") {
    try {
      arg3 = JSON.parse(arg3);
    } catch (err) {
      arg3 = null;
    }
    if (arg2 == "run") await tests.run(arg3);
  } else if (arg1 == "--finder") {
    if (arg2 == "open") await finder.get_open();
    if (arg2 == "scheduled") await finder.get_scheduled();
    if (arg2 == "test") await finder.test();
  } else if (arg1 == "v5") {
    await v5.main_runner(args);
  } else if (arg1 == "--temp") await temp.main_runner(args);
  else if (arg1 == "--tourney") {
    await tourney.main_runner(args);
  } else if (arg1 == "--hawku") {
    await hawku.main_runner();
  } else if (arg1 == "--fixers") {
    await fixers.main_runner();
  } else if (arg1 == "--mate") {
    await mate.main_runner();
  } else if (arg1 == "--race_speed_adj") {
    await race_speed_adj.main_runner();
  } else if (arg1 == "--helpers") {
    await helpers.main_runner();
  } else if (arg1 == "--transfers") {
    await transfers.main_runner();
  } else if (arg1 == "--tqual") {
    await tqual.main_runner();
  } else if (arg1 == "--stables") {
    await stables_s.main_runner();
  } else if (arg1 == "--sn_pro") {
    await sn_pro.main_runner();
  } else if (arg1 == "--send_weth") {
    await send_weth.main_runner();
  } else if (arg1 == "--hclass") {
    await hclass.main_runner();
  }

  console.log("---ed");
};
main(process.argv);
