const utils = require("pg/lib/utils");
const cyclic_depedency = require("../code/utils/cyclic_dependency");
const { cron_conf } = require("../code/utils/utils");
const b5_new_rngs = require("./b5_new_rngs");
const cron = require("node-cron");
const rng_ancestors = require("./rng_ancestors");

const crons_ar = {
  b5_new_rngs: [b5_new_rngs.cron_str, b5_new_rngs.runner],
  rng_ancestors: [rng_ancestors.cron_str, rng_ancestors.runner],
};
const runner = async (id) => {
  console.log("run cron", id);
  let ob = crons_ar[id];
  if (!ob) return console.log("ERR not found");
  let [cron_str, runner] = ob;
  console.log(id, "runner starting...");
  await runner();
};
const cron_ = async (id) => {
  console.log("##run cron", id);
  let ob = crons_ar[id];
  if (!ob) return console.log("ERR not found");
  let [cron_str, runner] = ob;
  cyclic_depedency.print_cron_details(cron_str);
  cron.schedule(cron_str, runner, cron_conf);
};

const main_runner = async (args) => {
  let [_node, _cfile, args1, arg2, arg3, arg4, arg5, arg6] = args;
  console.log("# temp ");
  if (arg2 == "runner") await runner(arg3);
  else if (arg2 == "cron") await cron_(arg3);
};

const temp_crons = {
  main_runner,
};
module.exports = temp_crons;
