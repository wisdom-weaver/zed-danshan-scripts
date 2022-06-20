const { fget } = require("./fetch");
const { getv } = require("./utils");

const mget = (api) =>
  fget(api, null, { "x-api-key": process.env.moralis_api_key });

const get_eth_usd = async () => {
  let resp = await mget(
    `https://deep-index.moralis.io/api/v2/erc20/0xdac17f958d2ee523a2206206994597c13d831ec7/price?chain=eth`
  );
  let tokval = parseFloat(getv(resp, "nativePrice.value"));
  let eth_usd = 1 / (tokval / 1e18);
  return { eth_usd };
};

const molaris = { get_eth_usd };
module.exports = molaris;
