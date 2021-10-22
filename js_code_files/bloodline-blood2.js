const { init, zed_ch, zed_db } = require("./index-run");
const _ = require("lodash");
const { get_ed_horse } = require("./zed_horse_scrape");
const { general_bulk_push } = require("./odds-generator-for-blood2");
let def_obl = {
  N: 0,
  S: 0,
  F: 0,
  B: 0,
};

const get_oblood_tag = async (hid) => {
  let oblood_doc = await zed_db.db.collection("oblood").findOne({ hid });
  let { oblood = null } = oblood_doc || {};
  return oblood;
};

const generate_oblood_tag = async (hid) => {
  try {
    hid = parseInt(hid);
    let doc = await zed_db.db
      .collection("horse_details")
      .findOne({ hid }, { hid: 1, parents: 1, _id: 0, bloodline: 1 });
    let { parents, bloodline } = doc;
    parents = _.chain(parents).values().compact().value();
    let oblood = {};
    if (_.isEmpty(parents)) {
      oblood = def_obl;
      oblood[bloodline.slice(0, 1)] = 100;
    } else {
      let oblood_s = await Promise.all(parents.map(get_oblood_tag));
      oblood_s = _.compact(oblood_s);
      if (_.isEmpty(oblood_s) || oblood_s.length == 1) continue
      oblood = ["N", "S", "F", "B"].map((b) => {
        let val = (oblood_s[0][b] + oblood_s[1][b]) / 2;
        return [b, val];
      });
      oblood = _.fromPairs(oblood);
    }
    console.log(hid, parents, oblood);
    return { hid, oblood };
  } catch (err) {
    console.log("err in generate_oblood_tag", err);
    return { hid, oblood: null };
  }
};

const oblood_tag_generator_all_horses = async () => {
  let ed_hid = await get_ed_horse();
  let [st, ed] = [1, ed_hid];
  // let [st, ed] = [19671, 19680];
  console.log(st, ed);
  let hids = new Array(ed - st + 1).fill(0).map((e, i) => i + st);
  let hids = new Array(ed - st + 1).fill(0).map((e, i) => i + st);
  let cs = 10;
  for (let chunk_hids of _.chunk(hids, cs)) {
    let obar = await Promise.all(chunk_hids.map(generate_oblood_tag));
    obar = _.compact(obar);
    if (!_.isEmpty(obar)) await general_bulk_push("oblood", obar);
    let [st_h, ed_h] = [chunk_hids[0], chunk_hids[chunk_hids.length - 1]];
    console.log("written bulk", obar.length, st_h, "->", ed_h);
  }
};

const runner = async () => {
  await init();
  await oblood_tag_generator_all_horses();
  console.log("done");
};
// runner();

module.exports = {};
