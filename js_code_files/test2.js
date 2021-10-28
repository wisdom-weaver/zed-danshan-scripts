const request = require("superagent");

const get = (id) => {
  return request
    .get(`https://api.zed.run/api/v1/horses/get/${id}`)
    .set("Content-Type", "application/json")
    .set("Origin", "https://zed.run")
    .set("Access-Control-Allow-Origin", "*")
    .set(
      "User-Agent",
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"
    )
    .set("Referer", "https://zed.run/")
    .then((response) => {
      return response.body;
    })
    .catch((err) => {
      console.log(err);
    });
};

const runner = async () => {
  let hdoc = await get(3312);
  console.log(hdoc);
};

runner();
