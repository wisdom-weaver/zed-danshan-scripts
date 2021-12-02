const request = require("superagent");
const getSales = async (offset, type) => {
  return (
    request
      .get(
        `https://api.opensea.io/api/v1/events?collection_slug=zed-run-official&event_type=${type}&only_opensea=false&offset=${offset}&limit=20`
      )
      //.get(`https://api.opensea.io/api/v1/events?collection_slug=zed-run-official&only_opensea=false&offset=${offset}&limit=20`)
      .set("Content-Type", "application/json")
      .then((response) => {
        return response.body.asset_events;
      })
      .catch((err) => {
        console.log(err);
      })
  );
};

const runner = async () => {
  let [type, offset] = ["created", 0];
  let ob = await getSales(offset, type);
  console.log(ob);
};

runner();
