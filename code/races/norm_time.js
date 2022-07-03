const _ = require("lodash");

const norm_preset_table = _.chain([
  // dist	count	mean	mi	mx	sd
  [1000, 57.53125162, 52.972, 61.4732, 0.3730371475],
  [1200, 71.20395781, 65.517, 75.0555, 0.5629665417],
  [1400, 83.43245636, 76.8171, 87.0156, 0.7583723073],
  [1600, 95.56369098, 89.3108, 101.9021, 1.103167773],
  [1800, 107.696281, 99.6484, 113.7688, 1.361802359],
  [2000, 119.7548351, 109.7824, 125.4245, 1.682022896],
  [2200, 131.8672145, 123.1638, 138.2836, 1.969148608],
  [2400, 143.974639, 133.8396, 151.6369, 2.602412432],
  [2600, 156.1284831, 145.5087, 165.9369, 3.414019718],
])
  .map((e) => {
    let [k, mean, mi, mx, sd] = e;
    return [k, { mean, mi, mx, sd }];
  })
  .fromPairs()
  .value();

const eval = (
  race,
  {
    time_key = "finishtime",
    dist_key = "distance",
    adjtime_key = "adjtime",
    race_time_key = "racetime",
  } = {}
) => {
  let times = _.map(race, time_key);
  let times_mean = _.mean(times);
  let dist = race[0][dist_key];
  let pre = norm_preset_table[dist];
  // console.log(pre);
  // console.log("times_mean", times_mean);
  let mean_diff_from_global = times_mean - pre.mean;
  // console.log("mean_diff_from_global", mean_diff_from_global);
  let diff_allowed = pre.sd;
  // console.log("diff_allowed", diff_allowed);
  let diff_sd_factor = mean_diff_from_global / diff_allowed;
  // console.log("diff_sd_factor", diff_sd_factor);
  race = race.map((r) => {
    let adjtime = r[time_key] - diff_sd_factor * diff_allowed;

    return {
      ...r,
      [adjtime_key]: adjtime,
      [race_time_key]: times_mean,
    };
  });
  return race;
};

const eval_norm_only = (finishtime, racetime, dist) => {
  let times_mean = racetime;
  let pre = norm_preset_table[dist];
  // console.log(pre);
  // console.log("times_mean", times_mean);
  let mean_diff_from_global = times_mean - pre.mean;
  // console.log("mean_diff_from_global", mean_diff_from_global);
  let diff_allowed = pre.sd;
  // console.log("diff_allowed", diff_allowed);
  let diff_sd_factor = mean_diff_from_global / diff_allowed;
  // console.log("diff_sd_factor", diff_sd_factor);
  let adjtime = finishtime - diff_sd_factor * diff_allowed;
  return adjtime;
};

const meth67_table = {
  1000: { t6: 57.57086259, t7: 57.63568977, t67mean: 57.63568977, fact: 1 },
  1200: {
    t6: 71.24295878,
    t7: 71.31171949,
    t67mean: 71.31171949,
    fact: 1.03141812,
  },
  1400: {
    t6: 83.47609738,
    t7: 83.54868612,
    t67mean: 83.54868612,
    fact: 1.03585102,
  },
  1600: {
    t6: 95.61032059,
    t7: 95.69615259,
    t67mean: 95.69615259,
    fact: 1.03832922,
  },
  1800: {
    t6: 107.7237292,
    t7: 107.8215945,
    t67mean: 107.8215945,
    fact: 1.03984722,
  },
  2000: {
    t6: 119.7604036,
    t7: 119.872628,
    t67mean: 119.872628,
    fact: 1.04114768,
  },
  2200: {
    t6: 131.9572636,
    t7: 132.0844164,
    t67mean: 132.0844164,
    fact: 1.04225978,
  },
  2400: {
    t6: 144.0485202,
    t7: 144.2163933,
    t67mean: 144.2163933,
    fact: 1.04259176,
  },
  2600: {
    t6: 156.2421209,
    t7: 156.4615938,
    t67mean: 156.4615938,
    fact: 1.04369422,
  },
};

const eval_norm_only_67meth = (finishtime, [t6, t7], dist) => {
  const t67mean = _.mean([t6, t7]);
  const tmean = meth67_table[dist].t67mean;
  let tadj = tmean - t67mean;
  let finishtime = r[7];
  let adjtime = finishtime + tadj;
  return adjtime;
};

const norm_time_s = {
  eval,
  eval_norm_only,
  eval_norm_only_67meth,
};
module.exports = norm_time_s;
