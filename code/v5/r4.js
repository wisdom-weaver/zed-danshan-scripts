const _ = require("lodash");
const { zed_db } = require("../connection/mongo_connect");
const { getv, dec } = require("../utils/utils");

const get_dp_doc = async (hid) => {
  try {
    hid = parseInt(hid);
    let dp4 =
      (await zed_db.db
        .collection("dp4")
        .findOne({ hid }, { projection: { dp: 1, dist: 1 } })) || {};
    if (!dp4 || _.isEmpty(dp4)) return null;
    let { dp, dist } = dp4;
    return {
      dp_score: dp,
      dp_dist: dist,
    };
  } catch (err) {
    return null;
  }
};

const struct_dp = (dp4) => {
  if (!dp4) return null;
  const { dp_score, dp_dist } = dp4;
  return `${dp_dist / 100}-${dec(dp_score, 2)}`;
};

const get_dp = async (hid) => {
  let dp4 = await get_dp_doc(hid);
  let dp = struct_dp(dp4);
  return dp;
};

const get_ymca_avgs = async ({ bloodline, breed_type, genotype }) => {
  let doc_id = "ymca2-global-avgs";
  let id = `${bloodline}-${breed_type}-${genotype}`;
  let doc = await zed_db.db
    .collection("requirements")
    .findOne({ id: doc_id }, { projection: { [`avg_ob.${id}`]: 1 } });
  // console.log(id);
  let this_ob = doc.avg_ob[id] || {};
  return this_ob;
};

const get_base_avg_hid = async (hid) => {
  let dets =
    (await zed_db.db
      .collection("horse_details")
      .findOne(
        { hid },
        { projection: { bloodline: 1, breed_type: 1, genotype: 1 } }
      )) || {};
  if (_.isEmpty(dets)) return null;
  let avgs = await get_ymca_avgs(dets);
  return avgs?.avg_base ?? null;
};

const get_ba = async (hid) => {
  try {
    hid = parseInt(hid);
    let ob =
      (await zed_db.db
        .collection("rating_blood3")
        .findOne({ hid }, { projection: { "base_ability.n": 1 } })) || {};
    let ba = getv(ob, "base_ability.n");
    if (ba) return dec(ba, 3);
    let avg_base = await get_base_avg_hid(hid);
    if (!avg_base) return null;
    return `${dec(avg_base, 2)} A`;
  } catch (err) {
    console.log("err", err.message);
    return null;
  }
};

const get_rng = async (hid) => {
  try {
    hid = parseInt(hid);
    let ob =
      (await zed_db.db
        .collection("gap4")
        .findOne({ hid }, { projection: { gap: 1 } })) || {};
    let gap = getv(ob, "gap");
    if (gap) return dec(gap, 3);
    return null;
  } catch (err) {
    console.log("err", err.message);
    return null;
  }
};

const get_r4 = async (hid) => {
  hid = parseInt(hid);
  const [dp, ba, rng] = await Promise.all([
    get_dp(hid),
    get_ba(hid),
    get_rng(hid),
  ]);
  return { hid, dp, ba, rng };
};

const r4data = {
  get_r4,
  get_ba,
  get_rng,
  get_dp,
  get_dp_doc,
  struct_dp,
};
module.exports = r4data;
