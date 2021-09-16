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
          "Bearer eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJjcnlwdG9maWVsZF9hcGkiLCJleHAiOjE2MzI2NjM5ODcsImlhdCI6MTYzMDI0NDc4NywiaXNzIjoiY3J5cHRvZmllbGRfYXBpIiwianRpIjoiNjI0MWI5YWItNjQ3My00YzFkLWJiMzItMmY3Y2Y5NzRhYjgzIiwibmJmIjoxNjMwMjQ0Nzg2LCJzdWIiOnsiZXh0ZXJuYWxfaWQiOiIzMjA4YmVmNy01OTRjLTRhYTgtOGU2YS0zNzJkMTNkY2I2NjMiLCJpZCI6MTQzNzAsInB1YmxpY19hZGRyZXNzIjoiMHhhMGQ5NjY1RTE2M2Y0OTgwODJDZDczMDQ4REExN2U3ZDY5RmQ5MjI0Iiwic3RhYmxlX25hbWUiOiJEYW5zaGFuIn0sInR5cCI6ImFjY2VzcyJ9.kp2RKawU5csgKe1MF4cB2Q27RLhqIr2GKErsgGxhCsa4GexHxtpx9OXvP2_pDETq74lHD6fECNHDC9Z7LhcnvA",
        Cookie:
          "__cf_bm=bYDCH4RM_wtk8XLli1MsSrwQt_ITjkISypq.MiEeACw-1631813537-0-AaYGcrHqOCQV61AYNqndwEpkluHSku2CBxtjcWQai8NAe5yZE9sdtLnNHKoJzUBFdm4uz8sUKWwkUmnhMl/7BPE=",
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
