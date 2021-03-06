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
const cdelay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

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
  // ["20", "fee_tag"],
  // ["21", "entryfee_usd"],
  // ["23", "pool"],
  // ["23", "pool"],
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

const fee_tags_ob = {
  A: [25.0, 17.5, 5000],
  B: [15.0, 12.5, 17.5],
  C: [10.0, 7.5, 12.5],
  D: [5.0, 3.75, 7.5],
  E: [2.5, 1.25, 3.75],
  F: [0.0, 0.0, 0.0],
};
const get_fee_tag = (entryfee_usd, f = 1) => {
  for (let [tag, [rep, mi, mx]] of _.entries(fee_tags_ob))
    if (_.inRange(entryfee_usd, mi, mx + 1e-3)) {
      if (f == 2) return rep;
      return tag;
    }
};
const disp_fee_tag = (tag) => {
  return "$" + dec2(fee_tags_ob[tag][0]);
};

const get_fee_tag_color = (fee) => {
  return (
    (fee == "A" && get_color("red")) ||
    (fee == "B" && get_color("pink")) ||
    (fee == "C" && get_color("blue")) ||
    (fee == "D" && get_color("fire")) ||
    (fee == "E" && get_color("green")) ||
    get_color("yellow")
  );
};

const dec_per = (a, b) => {
  if (b == 0) return `${dec(0)}%`;
  let per = (a * 100) / b;
  return `${dec(per)}%`;
};

const iso = (d = new Date()) => {
  try {
    if (parseInt(d) == d) d = parseInt(d);
    return new Date(d).toISOString();
  } catch (err) {
    return "iso-err";
  }
};
const nano = (d = new Date()) => {
  try {
    if (parseInt(d) == d) d = parseInt(d);
    d = new Date(d);
    return d.getTime();
  } catch (err) {
    return "nano-err";
  }
};

const per = (a, b) => {
  return `${parseFloat((a * 100) / b)?.toFixed(2)}%`;
};

const geno = (z) => {
  if (!z) return null;
  z = z.toString();
  if (z?.startsWith("Z")) z = z.slice(1);
  return parseFloat(z);
};

const get_hids = (st, ed) => {
  // console.log("gethids", st, ed);
  let hids = new Array(ed - st + 1).fill(0).map((e, i) => i + st);
  hids = hids.filter((hid) => ![15745, 15812].includes(hid));
  return hids;
};

const cron_conf = { scheduled: true };

const get_n = (n) => {
  if (!_.isNaN(parseFloat(n))) return parseFloat(n);
  return n;
};
const get_N = (n, e = 10) => {
  if (!_.isNaN(parseFloat(n))) return parseFloat(n);
  if (e === 10) return undefined;
  else return e;
};

const remove_bullshits = (ar) => {
  let n = ar.length;
  let nar = [];
  let idx_ar = new Array(n).fill(0).map((e, i) => i);
  for (let c = 0; c < n; c++) {
    let val = ar[c];
    nar[c] = { val };
  }

  const mean_diff = _.mean(_.compact(_.map(_.values(nar), "val")));
  // console.log(mean_diff)

  nar = nar.map((i, idx) => {
    let { val } = nar[idx];
    let onlys = _.filter(idx_ar, (i) => ar[i] !== val);
    onlys = _.compact(onlys.map((ec) => nar[ec].val || null));
    let ex_mean = _.mean(onlys);
    if (_.sum(onlys) == 0) ex_mean = mean_diff;
    let ex_val = mean_diff / ex_mean;
    let range_pm = 0.25;
    let bullshit = !_.inRange(ex_val, 1 - range_pm, 1 + range_pm);
    return { val, ex_mean, ex_val, bullshit };
  });
  nar = _.filter(nar, (i) => i.bullshit !== true);
  // console.table(nar);
  return _.map(nar, "val");
};

const getv = (ob, path) => {
  try {
    let a = _.get(ob, path);
    if (a === undefined) return undefined;
    return a;
  } catch (err) {
    return undefined;
  }
};

const promises_n = async (promises, n = 10) => {
  let ar = [];
  for (let chu of _.chunk(promises, n)) {
    let eachu = await Promise.all(chu);
    ar.push(eachu);
  }
  return _.flatten(ar);
};

const mt = 60 * 1000;

const utils = {
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
  fee_tags_ob,
  get_fee_tag,
  disp_fee_tag,
  get_fee_tag_color,
  dec_per,
  iso,
  nano,
  per,
  geno,
  get_hids,
  cron_conf,
  get_n,
  get_N,
  remove_bullshits,
  mt,
  getv,
  promises_n,
  cdelay,
};

module.exports = utils;
