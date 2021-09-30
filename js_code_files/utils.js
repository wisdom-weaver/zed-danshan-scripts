const fs = require("fs");
const path = require("path");
const app_root = require("app-root-path");
const _ = require("lodash");
const fetch = require("node-fetch");

const calc_avg = (ar = []) => {
  ar = _.compact(ar);
  if (_.isEmpty(ar)) return null;
  let sum = ar.reduce((acc, ea) => acc + ea, 0);
  return sum / ar.length;
};

const write_to_path = ({ file_path, data }) => {
  if (!fs.existsSync(path.dirname(file_path)))
    fs.mkdirSync(path.dirname(file_path), { recursive: true });
  fs.writeFileSync(file_path, JSON.stringify(data, null, 2));
};

const write_to_path_fast = ({ file_path, data }) => {
  if (!fs.existsSync(path.dirname(file_path)))
    fs.mkdirSync(path.dirname(file_path), { recursive: true });
  fs.writeFile(file_path, JSON.stringify(data, null, 2), {}, () => {});
};

const read_from_path = ({ file_path }) => {
  try {
    if (!fs.existsSync(file_path)) return null;
    json = fs.readFileSync(file_path, "utf8");
    if (_.isEmpty(json)) return null;
    return JSON.parse(json) || null;
  } catch (err) {
    return null;
  }
};
const dec2 = (n) => parseFloat(n).toFixed(2);
const pad = (n, l = 3) => {
  let pp = new Array(l - parseInt(n).toString().length).fill(0).join("");
  return `${pp}${n}`;
};

const calc_median = (array = []) => {
  array = _.compact(array);
  if (_.isEmpty(array)) return null;
  array = array.map(parseFloat).sort((a, b) => a - b);
  let median = 0;
  if (array.length == 0) return null;
  if (array.length % 2 === 0) {
    // console.log("even");
    let l = array[array.length / 2];
    let r = array[array.length / 2 - 1];
    // console.log({l,r})
    median = (l + r) / 2;
  } else {
    // console.log("odd");
    median = array[(array.length - 1) / 2]; // array with odd number elements
  }
  // console.log({array, median})
  return median;
};

const fetch_r_delay = 500;
const fetch_r = async (api, i = 3) => {
  if (i == 0) {
    console.log(`err fetching`, api);
    return null;
  }
  try {
    return await fetch(api).then((r) => r.json());
  } catch (err) {
    console.log(err.message, api);
    await delay(fetch_r_delay);
    return await fetch_r(api, i - 1);
  }
};

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

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
  ["16", "tc"],
];

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

const dec = (n, d = 2) => {
  if (n === null) return "null";
  if (!_.isNumber(parseFloat(n))) return;
  return parseFloat(n).toFixed(d);
};

const side_text = (side) =>
  (side == "B" && "good") ||
  (side == "A" && "bad") ||
  (side == "C" && "avg") ||
  "na";

module.exports = {
  calc_avg,
  write_to_path,
  read_from_path,
  dec2,
  pad,
  calc_median,
  fetch_r_delay,
  fetch_r,
  delay,
  struct_race_row_data,
  dec,
  side_text,
};
