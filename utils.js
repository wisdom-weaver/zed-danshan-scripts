const fs = require("fs");
const path = require("path");
const app_root = require("app-root-path");
const _ = require("lodash");

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
    let l = array[array.length / 2]
    let r = array[array.length / 2 - 1]
    // console.log({l,r})
    median = (l+r) / 2;
  } else {
    // console.log("odd");
    median = array[(array.length - 1) / 2]; // array with odd number elements
  }
  // console.log({array, median})
  return median;
};

module.exports = {
  calc_avg,
  write_to_path,
  read_from_path,
  dec2,
  pad,
  calc_median,
};
