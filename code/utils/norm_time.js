const _ = require("lodash");

const preset_table = _.chain([
  // dist	count	mean	mi	mx	sd
  [1000, 213012, 57.53125162, 52.972, 61.4732, 0.3730371475],
  [1200, 157956, 71.20395781, 65.517, 75.0555, 0.5629665417],
  [1400, 122640, 83.43245636, 76.8171, 87.0156, 0.7583723073],
  [1600, 109080, 95.56369098, 89.3108, 101.9021, 1.103167773],
  [1800, 100452, 107.696281, 99.6484, 113.7688, 1.361802359],
  [2000, 86400, 119.7548351, 109.7824, 125.4245, 1.682022896],
  [2200, 107592, 131.8672145, 123.1638, 138.2836, 1.969148608],
  [2400, 130140, 143.974639, 133.8396, 151.6369, 2.602412432],
  [2600, 163296, 156.1284831, 145.5087, 165.9369, 3.414019718],
])
  .map((e) => {
    let [k, mean, mi, mx, sd] = e;
    return [k, { mean, mi, mx, sd }];
  })
  .fromPairs()
  .value();

const eval = (
  race,
  { time_key = "finishtime", dist_key = "distance", adjtime_key = "adjtime" }
) => {
  let times = _.map(race, time_key);
  let times_mean = _.mean(times);
  let dist = race[0][dist_key];
  let pre = preset_table[dist];
  let mean_diff_from_global = times_mean - pre.mean;
  let diff_allowed = pre.sd;
  let diff_sd_factor = mean_diff_from_global / diff_allowed;
  race = race.map((r) => {
    let adjtime = r[time_key] - diff_sd_factor * diff_allowed;
    return { ...r, [adjtime_key]: adjtime };
  });
};

const norm_time_s = {
  eval,
};
module.exports = norm_time_s;
