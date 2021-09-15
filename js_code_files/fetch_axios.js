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
      return JSON.stringify(response.data);
    })
    .catch(function (error) {
      // console.log(error);
      return null;
    });
};

const fetch_fatigue = async (horse_id) => {
  try {
    let api = `https://api.zed.run/api/v1/horses/fatigue/${horse_id}`;
    let auth = `eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJjcnlwdG9maWVsZF9hcGkiLCJleHAiOjE2MzI2NjM5ODcsImlhdCI6MTYzMDI0NDc4NywiaXNzIjoiY3J5cHRvZmllbGRfYXBpIiwianRpIjoiNjI0MWI5YWItNjQ3My00YzFkLWJiMzItMmY3Y2Y5NzRhYjgzIiwibmJmIjoxNjMwMjQ0Nzg2LCJzdWIiOnsiZXh0ZXJuYWxfaWQiOiIzMjA4YmVmNy01OTRjLTRhYTgtOGU2YS0zNzJkMTNkY2I2NjMiLCJpZCI6MTQzNzAsInB1YmxpY19hZGRyZXNzIjoiMHhhMGQ5NjY1RTE2M2Y0OTgwODJDZDczMDQ4REExN2U3ZDY5RmQ5MjI0Iiwic3RhYmxlX25hbWUiOiJEYW5zaGFuIn0sInR5cCI6ImFjY2VzcyJ9.kp2RKawU5csgKe1MF4cB2Q27RLhqIr2GKErsgGxhCsa4GexHxtpx9OXvP2_pDETq74lHD6fECNHDC9Z7LhcnvA`;
    var config = {
      method: "get",
      url: api,
      headers: {
        Cookie: cookie_str,
        Authorization: `Bearer ${auth}`,
      },
    };
    let resp = await axios(config);
    let { current_fatigue = null } = resp?.data;
    if (current_fatigue !== null) current_fatigue = 100 - current_fatigue;
    return { current_fatigue };
  } catch (err) {
    return { current_fatigue: "err" };
  }
};

module.exports = { fetch_a, fetch_fatigue };
