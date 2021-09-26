require("dotenv").config();
const axios = require("axios");

const cookie_str =
  "__cf_bm=n.DfsfikmEgAMkx7QCIdeiSii8Ij_I3U7oyONbHlS1g-1631721161-0-Af4atYVCT2riaAEHMZSO2Y12Gi2E8jmrcOVjfmoa3wHXIxoWf0jEzPZix7JTR3GGH4H9GamI+6LS6haswQHujxo=";
const auth = process.env.zed_secret_key;

const config_def = {
  method: "get",
  headers: {
    Cookie: cookie_str,
  },
};

const fetch_a = async (api) => {
  let config = {
    url: api,
    ...config_def,
  };
  return axios(config)
    .then(function (response) {
      return response.data;
    })
    .catch(function (error) {
      // console.log(error);
      return null;
    });
};

const fetch_fatigue = async (hid) => {
  hid = parseInt(hid);
  try {
    let api_url = `https://api.zed.run/api/v1/horses/fatigue/${hid}`;
    let config = {
      method: "get",
      url: api_url,
      headers: {
        Authorization:
          "Bearer eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJjcnlwdG9maWVsZF9hcGkiLCJleHAiOjE2MzQ3Mzk0MjMsImlhdCI6MTYzMjMyMDIyMywiaXNzIjoiY3J5cHRvZmllbGRfYXBpIiwianRpIjoiNThlOTQxOGUtMWM2My00ZTE3LTlmZDMtNmU5NGE4NWVlYzRlIiwibmJmIjoxNjMyMzIwMjIyLCJzdWIiOnsiZXh0ZXJuYWxfaWQiOiIzMjA4YmVmNy01OTRjLTRhYTgtOGU2YS0zNzJkMTNkY2I2NjMiLCJpZCI6MTQzNzAsInB1YmxpY19hZGRyZXNzIjoiMHhhMGQ5NjY1RTE2M2Y0OTgwODJDZDczMDQ4REExN2U3ZDY5RmQ5MjI0Iiwic3RhYmxlX25hbWUiOiJEYW5zaGFuIn0sInR5cCI6ImFjY2VzcyJ9.g9vE8ocyLtal1HikX8XdLHUmUZtcbRKU5Bxm_b5TcIgJ2Rr5Sz35b3PWBeU8JQLzrGTYLRF0THHpCjVCDRJGjw",
        Cookie:
          "__cf_bm=cXS6Cy0zwxF6y.rB71OKOK8fJ2M0D0Ojlz25XbB9z8Y-1632683560-0-Ad39Lm3p3xGh1K5BVRZG//TRSeyLiGLRnkWWWvextVhLq6KpxXXoM+6L5DI1yBS95/O+rVQCa3v/3T4XKfmzbuo=",
      },
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
  let config = {
    method: "get",
    url: api_url,
    headers: {
      Cookie:
        "__cf_bm=IjPQYyifL8ZO4TjW1qQ63f.wtKxOx7zB.5g5on9tvoE-1631814461-0-AUuHDD4tjJ+HuyB8gYMoeaCntbxXBHUhCk4vCNNkLUde/d4Fb9lcZ7l147Y8cJVK19/6oYClfcgxfiKkFxjf//c=",
    },
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
