const axios = require("axios");

const zed_gql = "https://zed-ql.zed.run/graphql/getRaceResults";
const zed_secret_key =
  '"eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJjcnlwdG9maWVsZF9hcGkiLCJleHAiOjE2MzE5MzgzNzgsImlhdCI6MTYyOTUxOTE3OCwiaXNzIjoiY3J5cHRvZmllbGRfYXBpIiwianRpIjoiZTE1ZjczYWUtNjRkNi00ZjQ3LWI3MmItY2NlNTk1ZWU0ZDU0IiwibmJmIjoxNjI5NTE5MTc3LCJzdWIiOnsiZXh0ZXJuYWxfaWQiOiIzMjA4YmVmNy01OTRjLTRhYTgtOGU2YS0zNzJkMTNkY2I2NjMiLCJpZCI6MTQzNzAsInB1YmxpY19hZGRyZXNzIjoiMHhhMGQ5NjY1RTE2M2Y0OTgwODJDZDczMDQ4REExN2U3ZDY5RmQ5MjI0Iiwic3RhYmxlX25hbWUiOiJEYW5zaGFuIn0sInR5cCI6ImFjY2VzcyJ9.Ke5vgLKxoH9FVBUzD1PJ-qwPUH6EmMe_qQyS38MAJEkCroykb6ZRL2Hhu5TYsv0iCi5ta9wVx9R49FtjdB8v5g"';
const mt = 60 * 1000;

const get_zed_raw_data = async (from, to) => {
  try {
    let arr = [];
    let json = {};
    let headers = {
      "x-developer-secret": zed_secret_key,
      "Content-Type": "application/json",
    };

    let payload = {
      query: `query ($input: GetRaceResultsInput, $before: String, $after: String, $first: Int, $last: Int) {
  getRaceResults(before: $before, after: $after, first: $first, last: $last, input: $input) {
    edges {
      cursor
      node {
        name
        length
        startTime
        fee
        raceId
        status
        class

        horses {
          horseId
          finishTime
          finalPosition
          name
          gate
          ownerAddress
          class

        }
      }
    }
  }
}`,
      variables: {
        first: 500,
        input: {
          dates: {
            from: from,
            to: to,
          },
        },
      },
    };

    let axios_config = {
      method: "post",
      url: zed_gql,
      headers: headers,
      data: JSON.stringify(payload),
    };
    let result = await axios(axios_config);
    result = result?.data?.data?.getRaceResults?.edges || [];
    return result;
  } catch (err) {
    return [];
  }
};

const runner = async () => {
  console.log("runner");
  let ob = {};
  let now = Date.now();
  const from = new Date(now - mt * 5).toISOString();
  const to = new Date(now).toISOString();
  console.log("from:", from);
  console.log("to  :", to);
  ob = await get_zed_raw_data(from, to);
  console.log(ob);
  
};
runner();

module.exports = {
  zed_secret_key,
};
