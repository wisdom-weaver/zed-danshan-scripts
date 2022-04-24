const request = require("superagent");
require("dotenv").config();
const zed_secret_key = process.env.zed_secret_key;

const get = (api, auth = 0) => {
  let rr = request
    .get(api)
    .set("Content-Type", "application/json")
    .set("Origin", "https://zed.run")
    .set("Access-Control-Allow-Origin", "*")
    .set(
      "User-Agent",
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"
    )
    .set("Referer", "https://zed.run/");
  if (auth) rr = rr.set("authorization", `Bearer ${zed_secret_key}`);

  return rr
    .then((response) => {
      return response.body;
    })
    .catch((err) => {
      console.log(err.response.text);
      return null;
    });
};
const post = (api, body, auth = 1) => {
  let rr = request
    .post(api)
    .send(body)
    .set("Content-Type", "application/json")
    .set("Origin", "https://zed.run")
    .set("Access-Control-Allow-Origin", "*")
    .set(
      "User-Agent",
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"
    )
    .set("Referer", "https://zed.run/");
  if (auth) rr = rr.set("authorization", `Bearer ${zed_secret_key}`);

  return rr
    .then((response) => {
      return response.body;
    })
    .catch((err) => {
      console.log(err.response.text);
      return null;
    });
};

const horse = async (hid) => {
  return get(`https://api.zed.run/api/v1/horses/get/${hid}`);
};
const horses = async (hids = []) => {
  let query = hids.map((h) => `horse_ids[]=${h}`).join("&");
  let url = `https://api.zed.run/api/v1/horses/get_horses?${query}`;
  // console.log(url)
  return get(url);
};
const fatigue = async (hid) => {
  let api = `https://api.zed.run/api/v1/horses/fatigue/${hid}`;
  return get(api, 1);
};
const race = async (rid) => {
  let api = `https://racing-api.zed.run/api/v1/races?race_id=${rid}`;
  return get(api);
};
const race_results = async (rid) => {
  let api = `https://racing-api.zed.run/api/v1/races/result/${rid}`;
  return get(api);
};
const race_flames = async (rid) => {
  let api = `https://rpi.zed.run/?race_id=${rid}`;
  return get(api);
};
const zedf = {
  get,
  post,
  horse,
  horses,
  fatigue,
  race,
  race_results,
  race_flames,
};
module.exports = zedf;
