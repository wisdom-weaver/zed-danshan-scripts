const fetch = require("node-fetch");
const axios = require("axios");
const _ = require("lodash");

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const fetch_r = async (api, i = 2) => {
  try {
    if (i == 0) return null;
    return await fetch(api).then((r) => r.json());
  } catch (err) {
    return await fetch_r(api, i - 1);
  }
};

const api_zed_hid = async (hid) => {
  let api = `https://api.zed.run/api/v1/horses/get/${hid}`;
  return await fetch_r(api);
};

const sn_bk = "https://api.stackednaks.com";
// const sn_bk = "http://localhost:3001";

const sn_fget = async (api) => {
  const config = {
    method: "get",
    url: api,
    headers: {
      "x-auth": process.env.ADMIN_KEY,
    },
  };

  let resp = await axios(config);
  if (!resp) return null;
  return resp?.data || null;
};

const fget = (api, a, headers = {}) =>
  fetch(api, {
    method: "GET",
    headers: { "Content-Type": "application/json", ...headers },
  })
    .then((r) => r.json())
    .catch((err) => {
      // console.log(err);
    });
const fpost = (api, data) =>
  fetch(api, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(data),
  }).then((r) => r.json());

module.exports = {
  delay,
  fetch_r,
  api_zed_hid,
  sn_fget,
  sn_bk,
  fget,
  fpost,
};
