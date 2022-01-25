const {
  get_fee_cat_on,
  download_eth_prices,
  get_entryfee_usd,
} = require("./base");
const _ = require("lodash");
const { zed_ch, init, zed_db } = require("../connection/mongo_connect");
const { get_fee_tag } = require("./utils");

const key_mapping_bs_zed = [
  ["_id", "_id"],
  ["1", "distance"],
  ["2", "date"],
  ["3", "entryfee"],
  ["4", "raceid"],
  ["5", "thisclass"],
  ["6", "hid"],
  ["7", "finishtime"],
  ["8", "place"],
  ["9", "name"],
  ["10", "gate"],
  ["11", "odds"],
  ["12", "unknown"],
  ["13", "flame"],
  ["14", "fee_cat"],
  ["15", "adjfinishtime"],
  ["16", "htc"],
  ["17", "race_name"],
];

const from_ch_zed_collection = async (query) => {
  try {
    let data = await zed_ch.db.collection("zed").find(query).toArray();
    data = _.uniqBy(data, (i) => [i["4"], i["6"]].join());
    return data;
  } catch (err) {
    return [];
  }
};

const get_tunnel = (dist) => {
  if (dist >= 1000 && dist <= 1400) return "S";
  if (dist >= 1600 && dist <= 2000) return "M";
  if (dist >= 2200 && dist <= 2600) return "D";
  return null;
};

const struct_race_row_data = (data) => {
  try {
    // console.log(data.length);
    if (_.isEmpty(data)) return [];
    data = data?.map((row) => {
      // console.log(row);
      if (row == null) return null;
      return key_mapping_bs_zed.reduce(
        (acc, [key_init, key_final]) => ({
          ...acc,
          [key_final]: row[key_init] || 0,
        }),
        {}
      );
    });
    data = _.compact(data);
    data = data.map((e) => {
      let { entryfee: fee, date } = e;
      let entryfee_usd = get_entryfee_usd({ fee, date });
      let fee_tag = get_fee_tag(entryfee_usd);
      let tunnel = get_tunnel(e.distance);
      return { ...e, entryfee_usd, fee_tag, tunnel };
    });
  } catch (err) {
    if (data.name == "MongoNetworkError") {
      console.log("MongoNetworkError");
    }
    return [];
  }
  return data;
};

const get_races_of_hid = async (hid) => {
  hid = parseInt(hid);
  if (isNaN(hid)) return [];
  hid = parseInt(hid);
  let query = { 6: hid };
  let data = await from_ch_zed_collection(query);
  data = struct_race_row_data(data);
  return data;
};

const progress_bar = (a, b) => {
  let len = 50;
  let per = parseFloat((a / b) * 100).toFixed(2);
  let eqs = new Array(Math.ceil((len * a) / b)).fill("=").join("");
  let dts = new Array(Math.ceil(len * (1 - a / b))).fill(".").join("");
  return `[${eqs}>${dts}] ${per}%| ${a}/${b}`;
};

const initiate = async () => {
  await init();
  await download_eth_prices();
};

const general_bulk_push = async (coll, obar) => {
  try {
    if (_.isEmpty(obar)) return;
    let bulk = [];
    for (let ob of obar) {
      if (_.isEmpty(ob)) continue;
      let { hid } = ob;
      bulk.push({
        updateOne: {
          filter: { hid },
          update: { $set: ob },
          upsert: true,
        },
      });
    }
    await zed_db.db.collection(coll).bulkWrite(bulk);
    let len = obar.length;
    let sth = obar[0].hid;
    let edh = obar[obar.length - 1].hid;
    console.log("wrote bulk", coll, len, "..", sth, "->", edh);
  } catch (err) {
    console.log("err mongo bulk", coll, coll, obar && obar[0]?.hid);
    console.log(err);
  }
};

const get_ed_horse = async () => {
  let end_doc = await zed_db.db
    .collection("horse_details")
    .find({ hid: { $type: 16 } }, { projection: { _id: 0, hid: 1 } })
    .sort({ hid: -1 })
    .limit(1)
    .toArray();
  end_doc = end_doc && end_doc[0];
  return end_doc?.hid;
};

const get_all_hids = async () => {
  let ed = await get_ed_horse();
  let st = 1;
  let hids = new Array(ed - st + 1).fill(0).map((e, i) => i + st);
  return hids;
};

const cyclic_depedency = {
  get_races_of_hid,
  from_ch_zed_collection,
  struct_race_row_data,
  progress_bar,
  initiate,
  general_bulk_push,
  get_ed_horse,
  get_all_hids,
};

module.exports = cyclic_depedency;
