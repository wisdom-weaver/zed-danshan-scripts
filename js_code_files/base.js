var app_root = require("app-root-path");
const _ = require("lodash");
const { zed_db } = require("./index-run");
const { read_from_path, write_to_path } = require("./utils");
const cron = require("node-cron");
const cron_parser = require("cron-parser");

let eth_prices_file_path = `${app_root}/required_jsons/eth-usd-price.json`;

let eth_prices = read_from_path({
  file_path: eth_prices_file_path,
});

const download_eth_prices = async () => {
  let doc = await zed_db.collection("base").findOne({ id: "eth_prices" });
  let doc_local = read_from_path({
    file_path: eth_prices_file_path,
  });

  console.log("_ETH:: Now         :", new Date().toISOString());
  console.log(
    "_ETH:: Last Cache  :",
    new Date(doc_local.db_date).toISOString()
  );
  console.log("_ETH:: Databse Date:", new Date(doc.db_date).toISOString());

  let diff =
    new Date(doc_local.db_date).getTime() - new Date(doc.db_date).getTime();
  if (Math.abs(diff) < 1 * 1000) {
    console.log("ETH prices latest=>", _.keys(doc.data)[0]);
    return;
  }
  write_to_path({
    file_path: eth_prices_file_path,
    data: doc,
  });
  eth_prices = doc;
  console.log("download ETH prices sucessfull, latest=>", _.keys(doc.data)[0]);
  let date = get_date(0);
  console.log({ [date]: get_at_eth_price_on(date) });
};

const dollar_limit_breaks = {
  C_B: 4,
  B_A: 9,
};
const get_price_limits = (eth_price) => {
  let eth_limits = _.entries(dollar_limit_breaks).map(([f, bre]) => [
    f,
    bre / eth_price,
  ]);
  eth_limits = Object.fromEntries(eth_limits);
  return eth_limits;
};
const get_price_limits_on_date = (date) => {
  date = get_date(date);
  let price = get_at_eth_price_on(date);
  return get_price_limits(price);
};
const get_date = (date) => {
  try {
    if (!date || date == 0) date = Date.now();
    if (!isNaN(parseFloat(date))) date = new Date(date).toISOString();
    date = date.slice(0, 10);
    return date;
  } catch (err) {
    return get_date(0);
  }
};
const get_at_eth_price_on = (date) => {
  let price = eth_prices.data[date];
  return price || _.values(eth_prices.data)[0];
};
const get_fee_cat = ({ price, fee = 0 }) => {
  fee = parseFloat(fee);
  let price_limits = get_price_limits(price);
  let [l1, l2] = _.values(price_limits);
  // console.log({ price_limits });
  if (fee == 0) return "F";
  if (fee < l1) return "C";
  else if (fee >= l1 && fee < l2) return "B";
  else if (fee >= l2) return "A";
  return "N";
};
const get_fee_cat_on = ({ date, fee }) => {
  date = get_date(date);
  let price = get_at_eth_price_on(date);
  let fee_cat = get_fee_cat({ price, fee });
  // console.log({ fee, date, fee_cat });
  return fee_cat;
};
const eth_runner_fn = async () => {
  await download_eth_prices();
};
const auto_eth_cron = async () => {
  let cron_str = "0 11 0 * * *";
  console.log("#starting auto_eth_cron", cron_str, cron.validate(cron_str));
  const c_itvl = cron_parser.parseExpression(cron_str);
  const runner = () => {
    console.log("#running auto_eth_cron");
    console.log("_ETH: Now run:", new Date().toISOString());
    console.log("_ETH: Next run:", c_itvl.next().toISOString());
    eth_runner_fn();
  };
  runner();
  cron.schedule(cron_str, runner);
};

module.exports = {
  get_price_limits,
  get_price_limits_on_date,
  get_date,
  get_at_eth_price_on,
  get_fee_cat_on,
  download_eth_prices,
  auto_eth_cron,
};
