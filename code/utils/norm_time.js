const _ = require("lodash");

const preset_table = _.chain([
  ["1600_0_0", 95.6699, 91.9024, 98.1669, 0.8634],
  ["1600_1_0", 95.2815, 89.3108, 99.3537, 1.2956],
  ["1600_2_0", 95.4955, 91.1131, 99.395, 1.0054],
  ["1600_3_0", 95.5904, 90.3684, 99.1928, 0.9853],
  ["1600_4_0", 95.7549, 90.4109, 100.1038, 0.8267],
  ["1600_5_0", 95.9518, 91.5531, 98.7152, 0.7205],
  ["1600_6_0", 96.3069, 92.8144, 98.45, 0.6625],
  ["1600_99_0", 95.4158, 90.0207, 98.8606, 1.1711],
  ["1600_1000_0", 95.5477, 89.6488, 98.6481, 1.0891],
  ["1600_1_1", 95.276, 90.0802, 100.973, 1.7102],
  ["1600_2_1", 95.4712, 90.7784, 98.6864, 1.3645],
  ["1600_3_1", 95.5115, 89.7473, 99.0786, 1.3912],
  ["1600_4_1", 95.6928, 89.9662, 101.9021, 1.2018],
  ["1600_5_1", 95.8028, 89.4446, 99.5581, 1.0547],
  ["1600_6_1", 96.044, 93.1792, 99.8117, 0.8058],
])
  .map((e) => {
    let [k, mean, mi, mx, sd] = e;
    return [k, { mean, mi, mx, sd }];
  })
  .fromPairs()
  .value();

const eval = (rrow)=>{
  let times = _.map(race, "finishtime");
  let times_mean = _.mean(times);
  let dist = race[0].distance;
  let pre = preset_global[dist];
  let mean_diff_from_global = times_mean - pre.mean;
  let diff_allowed = pre.sd;
  let diff_sd_factor = mean_diff_from_global / diff_allowed;
  race = race.map((r) => {
    let adjtime = r.finishtime - diff_sd_factor * diff_allowed;
    return { ...r, adjtime };
  });
}

const norm_time_s = {};
module.exports = norm_time_s;
