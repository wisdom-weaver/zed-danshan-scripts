const speed_shoe = require("./speed_shoe_global");

const main_runner = async () => {
  let [_node, _cfile, v, arg1, arg2, arg3, arg4, arg5] = process.argv;
  console.log(v);
  if (arg1 == "speed_shoe") await speed_shoe.main_runner();
};

const helpers = { main_runner };

module.exports = helpers;
