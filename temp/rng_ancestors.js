const cyclic_depedency = require("../code/utils/cyclic_dependency");
const { iso } = require("../code/utils/utils");
const v5_conf = require("../code/v5/v5_conf");
const v3rng = require("../code/v3/gaps");
const _ = require("lodash");
const sheet_ops = require("../sheet_ops/sheets_ops");
const { zed_db } = require("../code/connection/mongo_connect");
const r4data = require("../code/v5/r4");
const { get_ancesters } = require("../code/v5/ancestors");

let range = "B5_BABY_RNG!A1";
let spreadsheetId = "1M4pC0dcTeqek6gj0mMSwAHoGt60fMgTh_wcWr72fdI8";

const get_hid_ans_rng = async ({ hid }) => {
  try {
    let baby_rng = await r4data.get_rng(hid);
    let ans = await get_ancesters({ hid });
    ans = _.filter(ans, (i) => i[2] <= 5);
    let ans_rngs = await Promise.all(
      _.map(ans, 0).map((h) => r4data.get_rng(h).then((d) => [h, d]))
    );
    ans_rngs = _.fromPairs(ans_rngs);
    let ob = ans.map(([hid, k, level]) => {
      let rng = ans_rngs[hid];
      return { hid, k, level, rng };
    });
    ob = [{ hid, k: "baby", level: 0, rng: baby_rng }, ...ob];
    ob = _.chain(ob)
      .keyBy("k")
      .mapValues((e) => parseFloat(e?.rng))
      .value();
    ob = { hid, ...ob };
    // console.table([ob]);
    return ob;
  } catch (err) {
    return {};
  }
};

const get_hids_rng = async () => {
  const cs = 30;
  let docs =
    (await zed_db.db
      .collection("horse_details")
      .find(
        { tx_date: { $gte: v5_conf.st_date }, hid: { $gte: 380000 } },
        { projection: { hid: 1 } }
      )
      .toArray()) || [];
  // docs = docs.slice(0, cs * 2);
  console.log("babies:", docs.length);
  let hids = _.map(docs, "hid");
  hids = [63797, ...hids];
  let obar = [];
  for (let chu of _.chunk(hids, cs)) {
    console.log(chu[0], "==>", chu[chu.length - 1]);
    let ar = await Promise.all(chu.map((h) => get_hid_ans_rng({ hid: h })));
    obar.push(ar);
  }
  obar = _.flatten(obar);
  obar = _.compact(obar);
  // console.table(obar);
  return obar;
};

const runner = async () => {
  let data = await get_hids_rng();
  if (_.isEmpty(data)) return console.log("no data found");
  let resp = await sheet_ops.sheet_print_ob(data, {
    spreadsheetId: "1MWnILjDr71rW-Gp8HrKP6YnS03mJARygLSuS7xxsHhM",
    range: `For Orange!A1`,
  });
  console.log(iso(), "completed", data);
  console.log(resp.data);
};

// const cron_str = "* * * * *";
const cron_str = "*/20 * * * *";
const rng_ancestors = {
  cron_str,
  runner,
};
module.exports = rng_ancestors;
