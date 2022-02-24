const qs = require("query-string");
const { fget } = require("../utils/fetch");
require("dotenv").config();

const danshan_eth_address = process.env.danshan_eth_address;
const etherscan_api_key = process.env.etherscan_api_key;
const weth_contract_address = process.env.weth_contract_address;

const eth = `https://api.etherscan.io`;

const eth_get = (query) => {
  try {
    query.apikey = etherscan_api_key;
    let api = `${eth}/api?${qs.stringify(query)}`;
    return fget(api);
  } catch (err) {
    console.error("err at eth-get", err.message);
    return null;
  }
};

const get_tx_status = ({ txhash }) => {
  let query = {
    module: "transaction",
    action: "gettxreceiptstatus",
    txhash,
  };
  return eth_get(query);
};
const get_eth_txs = ({ address }) => {
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
  return eth_get(query);
};
const get_eth_balance = ({ address }) => {
  let query = {
    module: "account",
    action: "tokenbalance",
    address: address,
    contractaddress,
    tag: "latest",
  };
  return eth_get(query);
};

const etherscan = {
  eth_get,
  get_eth_txs,
  get_eth_balance,
  get_tx_status,
};
module.exports = etherscan;
