const { get_fee_cat_on, download_eth_prices } = require("./base");
const _ = require("lodash");
const { zed_ch, init } = require("./index-run");

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
  data = data.map((e) => {
    let { entryfee: fee, date } = e;
    let fee_cat = get_fee_cat_on({ fee, date });
    return { ...e, fee_cat };
  });
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

module.exports = {
  get_races_of_hid,
  from_ch_zed_collection,
  struct_race_row_data,
  progress_bar,
  initiate,
};
