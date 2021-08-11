const eth_prices = require("./required_jsons/eth-usd-price.json");
const _ = require("lodash");

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
  let price = eth_prices[date];
  return price || _.values(eth_prices)[0];
};
const get_fee_cat = ({ price, fee = 0 }) => {
  fee = parseFloat(fee);
  let price_limits = get_price_limits(price);
  let [l1, l2] = _.values(price_limits);
  // console.log({ price_limits });
  if(fee==0) return "F";
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

module.exports = {
  get_price_limits,
  get_price_limits_on_date,
  get_date,
  get_at_eth_price_on,
  get_fee_cat_on,
};
