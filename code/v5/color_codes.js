const _ = require("lodash");

const code_cn_ob = {
  r: "bg-red-500",
  o: "bg-orange-500",
  y: "bg-yellow-500 text-black",
  g: "bg-green-500",
  p: "bg-purple-600",
  b: "bg-blue-500",
  bk: "bg-black",
};
const bg_code_cn_ob = {
  r: "bg-red-500",
  o: "bg-orange-500",
  y: "bg-yellow-500 text-black",
  g: "bg-green-500",
  p: "bg-purple-600",
  pi: "bg-pink-600",
  b: "bg-blue-500",
  bk: "bg-black",
  dk: "bg-dk",
  pk: "bg-pk",
  c0: "bg-c0",
  c1: "bg-c1",
  c2: "bg-c2",
  c3: "bg-c3",
  c4: "bg-c4",
  c5: "bg-c5",
  c6: "bg-c6",
  c7: "bg-c7",
  c99: "bg-c99",
};
const text_code_cn_ob = {
  r: "text-red-500",
  o: "text-orange-500",
  y: "text-yellow-500",
  g: "text-green-500",
  p: "text-purple-600",
  pi: "text-pink-600",
  b: "text-blue-500",
  bk: "text-black",
  dk: "text-dk",
  pk: "text-pk",
  c0: "text-c0",
  c1: "text-c1",
  c2: "text-c2",
  c3: "text-c3",
  c4: "text-c4",
  c5: "text-c5",
  c6: "text-c6",
  c7: "text-c7",
  c99: "text-c99",
};
const code_cn = (c) => code_cn_ob[c];

const speed_dist_ob = {
  1000: 62.42549974,
  1200: 60.523951221,
  1400: 60.264939971,
  1600: 60.121104911,
  1800: 60.033337925,
  2000: 59.958352854,
  2200: 59.894376615,
  2400: 59.875304908,
  2600: 59.81205861,
};
const speed_fact = { y: 1.033, r: 1.04 };
const get_speed_code = (speed, rdist) => {
  try {
    if (!speed || _.isNaN(speed)) return "bk";
    if (speed >= speed_dist_ob[rdist] * speed_fact.r) return "r";
    if (speed >= speed_dist_ob[rdist] * speed_fact.y) return "y";
    return "g";
  } catch (err) {
    return "b";
  }
};

const class_cn = (c) => {
  return `bg-c${c}`;
};

const get_rng_code = (rng) => {
  // <.25	Blue
  // <.5	Green
  // <.8	Yellow
  // <1.2	Orange
  // >1.2	RED
  rng = parseFloat(rng);
  if ([null, NaN, undefined].includes(rng)) return "bk";
  if (rng <= 0.25) return "b"; //Blue
  if (rng <= 0.5) return "g"; //Green
  if (rng <= 0.8) return "y"; //Yellow
  if (rng <= 1.2) return "o"; //Orange
  if (rng >= 1.2) return "r"; //RED
  return "b";
};

const dp_dist_ob = {
  10: [2.46, 3.12],
  12: [1.91, 2.43],
  14: [1.48, 1.89],
  16: [0.72, 0.91],
  18: [1.44, 1.83],
  20: [1.87, 2.38],
  22: [2.35, 2.99],
  24: [2.46, 3.13],
  26: [2.58, 3.28],
};

const dp_dist_allowed = {
  1000: [12, 14],
  1200: [10, 12, 14],
  1400: [10, 12, 16, 18],
  1600: [14, 18, 20],
  1800: [20, 22, 16, 14],
  2000: [16, 18, 22, 24, 26],
  2200: [20, 18, 24, 26],
  2400: [22, 20, 26],
  2600: [22, 20, 24],
};

const get_dp_code = (dp) => {
  try {
    if (!dp) return "bk";
    let [dp_dist, score] = dp.split("-");
    dp_dist = parseFloat(dp_dist);
    score = parseFloat(score);
    if (dp_dist == 0) return "bk";
    // console.log(dp_dist, rdist, Math.abs(dp_dist - rdist / 100));

    if (score >= dp_dist_ob[dp_dist][1]) return "r";
    else if (score >= dp_dist_ob[dp_dist][0]) return "y";
    else return "g";
  } catch (err) {
    console.log(err);
    return "bk";
  }
};
const get_ba_code = (ba) => {
  try {
    ba = parseFloat(ba);
    if (!ba || _.isNaN(ba)) return "cbg-black";
    // if (ba.includes(" A")) return "bg-slate-600";
    if (ba >= 4) return "r";
    if (ba >= 3) return "o";
    if (ba >= 2) return "y";
    if (ba >= 1) return "g";
    return "b";
  } catch (err) {
    console.log(err);
    return "b";
  }
};

const get_rng_cn = (n) => code_cn(get_rng_code(n));
const get_dp_cn = (...n) => code_cn(get_dp_code(...n));
const get_ba_cn = (n) => code_cn(get_ba_code(n));

const code_score_chart = {
  rng: { b: 1, g: 2, y: 3, o: 4, r: 5, bk: 0 },
  dp: { b: 0, g: 1, y: 3, o: 0, r: 5, bk: 0 },
  ba: { b: 1, g: 2, y: 3, o: 4, r: 5, bk: 0 },
};

const get_rng_val = (...n) => code_score_chart["rng"][get_rng_code(...n)] ?? 0;
const get_dp_val = (...n) => code_score_chart["dp"][get_dp_code(...n)] ?? 0;
const get_ba_val = (...n) => code_score_chart["ba"][get_ba_code(...n)] ?? 0;

const cc = {
  bg_code_cn_ob,
  text_code_cn_ob,
  code_cn,
  get_speed_code,
  class_cn,
  get_rng_code,
  get_dp_code,
  get_ba_code,
  get_rng_cn,
  get_dp_cn,
  get_ba_cn,
  get_rng_val,
  get_dp_val,
  get_ba_val,
};
module.exports = cc;
