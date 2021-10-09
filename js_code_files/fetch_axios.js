require("dotenv").config();
const axios = require("axios");

const auth = process.env.zed_secret_key;
const headers = {
  Authorization:
    "Bearer eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJjcnlwdG9maWVsZF9hcGkiLCJleHAiOjE2MzYyMDk4NDMsImlhdCI6MTYzMzc5MDY0MywiaXNzIjoiY3J5cHRvZmllbGRfYXBpIiwianRpIjoiMTY5MzViODgtZWQ1MS00NzlkLThkMWQtMzRlNmFhZTVkYmU0IiwibmJmIjoxNjMzNzkwNjQyLCJzdWIiOnsiZXh0ZXJuYWxfaWQiOiIzMjA4YmVmNy01OTRjLTRhYTgtOGU2YS0zNzJkMTNkY2I2NjMiLCJpZCI6MTQzNzAsInB1YmxpY19hZGRyZXNzIjoiMHhhMGQ5NjY1RTE2M2Y0OTgwODJDZDczMDQ4REExN2U3ZDY5RmQ5MjI0Iiwic3RhYmxlX25hbWUiOiJEYW5zaGFuIn0sInR5cCI6ImFjY2VzcyJ9.b3lw8F5a2BWI3gD3K5ELNc1uBbWp2MVljLFxcrommGCpHG5s1Ue1M19MRu1yVnZc4sgQ4ETa0a3YvsqDjTCwAQ",
  Cookie:
    "__cf_bm=YFcBLkPRQ7B2WCXO.yMyAL4rDzE7lh3Hl_EaIjAqO1Y-1633791139-0-AYlVVK3DoG3mCmyiTQiM1NhrEDiyrAu6sq1pINEDkn8aoJT5juu/wQqd7Z8fP6EOoTCIEJzIpT0pb5sS+s1YD4E=",
};
const fetch_a = async (api) => {
  var config = {
    method: "get",
    url: api,
    headers,
  };
  return axios(config)
    .then(function (response) {
      return response.data;
    })
    .catch(function (error) {
      // console.log("err", api, error);
      console.log("err", api);
      return null;
    });
};

const fetch_fatigue = async (hid) => {
  hid = parseInt(hid);
  try {
    let api_url = `https://api.zed.run/api/v1/horses/fatigue/${hid}`;
    var config = {
      method: "get",
      url: api_url,
      headers,
    };

    return axios(config)
      .then(function (response) {
        let ob = response.data;
        ob = { hid, ...ob };
        return ob;
      })
      .catch(function (error) {
        // console.log(error);
        return null;
      });
  } catch (err) {
    return { hid, current_fatigue: "err" };
  }
};

const fetch_horse_zed_api = async (hid) => {
  hid = parseInt(hid);
  let api_url = `https://api.zed.run/api/v1/horses/get/${hid}`;
  var config = {
    method: "get",
    url: api_url,
    headers,
  };
  return axios(config)
    .then(function (response) {
      let ob = response.data;
      let { class: thisclass, hash_info, rating, win_rate } = ob;
      let { name } = hash_info;
      ob = { hid, thisclass, name, rating, win_rate };
      return ob;
    })
    .catch(function (error) {
      // console.log(error);
      return null;
    });
};

module.exports = { fetch_a, fetch_fatigue, fetch_horse_zed_api };
