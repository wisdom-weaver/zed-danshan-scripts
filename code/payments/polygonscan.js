const qs = require("query-string");
const { fget } = require("../utils/fetch");
require("dotenv").config();

const danshan_eth_address = process.env.danshan_eth_address;
const polygonscan_api_key = process.env.polygonscan_api_key;
const weth_contract_address = process.env.weth_contract_address;

const pol = `https://api.polygonscan.com`;

const pol_get = (query) => {
  try {
    query.apikey = polygonscan_api_key;
    let api = `${pol}/api?${qs.stringify(query)}`;
    console.log(api);
    return fget(api);
  } catch (err) {
    console.error("err at pol-get", err.message);
    return null;
  }
};

const get_tx_status = ({ txhash }) => {
  let query = {
    module: "transaction",
    action: "gettxreceiptstatus",
    txhash,
  };
  return pol_get(query);
};
const get_token_txs = ({
  address,
  contractaddress,
  startblock = 0,
  endblock = 99999999,
}) => {
  let query = {
    module: "account",
    action: "tokentx",
    // address: address,
    contractaddress,
    startblock,
    endblock,
    // page: 0,
    // offset: 0,
    sort: "desc",
  };
  return pol_get(query);
};
const get_weth_txs = ({ address, startblock, endblock }) => {
  return get_token_txs({
    address,
    contractaddress: weth_contract_address,
    startblock,
    endblock,
  });
};
const get_matic_txs = ({ address }) => {
  let query = {
    module: "account",
    action: "txlist",
    address: address,
    startblock: 0,
    endblock: 99999999,
    page: 1,
    offset: 0,
    sort: "desc",
  };
  return pol_get(query);
};

const get_token_balance = ({ address, contractaddress }) => {
  let query = {
    module: "account",
    action: "tokenbalance",
    address: address,
    contractaddress,
    tag: "latest",
  };
  return pol_get(query);
};
const get_weth_balance = ({ address }) => {
  return get_token_balance({ address, contractaddress: weth_contract_address });
};
const get_matic_balance = ({ address }) => {
  let query = {
    module: "account",
    action: "tokenbalance",
    address: address,
    contractaddress,
    tag: "latest",
  };
  return pol_get(query);
};

const moralis_get_weth_txs = ({
  address,
  from_date = moment().add(-2, "minutes").toISOString(),
  to_date = iso(),
}) => {
  let base = `https://deep-index.moralis.io/api/v2/${address}/erc20/transfers`;
  const qqr = {
    chain: "polygon",
    from_date: from_date,
    to_date: to_date,
  };
  // console.log(qqr)
  let api = `${base}?${qs.stringify(qqr)}`;
  // console.log(api);
  let api_key = process.env.moralis_api_key;
  return fget(api, null, { "x-api-key": api_key });
};

const polygonscan = {
  pol_get,
  get_tx_status,
  get_token_txs,
  get_matic_txs,
  get_weth_txs,
  get_token_balance,
  get_matic_balance,
  get_weth_balance,
  moralis_get_weth_txs,
};
module.exports = polygonscan;
