const _ = require("lodash");
const { fetch_r } = require("./utils.js");
const { init, zed_ch, zed_db } = require("./index-run");
const {
  from_ch_zed_collection,
  struct_race_row_data,
  get_races_of_hid,
} = require("./cyclic_dependency");

let global = {
  1000: { mean: 57.72, sd: 0.86 },
  1200: { mean: 71.41, sd: 1.08 },
  1400: { mean: 83.61, sd: 1.28 },
  1600: { mean: 95.77, sd: 1.47 },
  1800: { mean: 107.87, sd: 1.64 },
  2000: { mean: 119.9, sd: 1.87 },
  2200: { mean: 131.97, sd: 2.15 },
  2400: { mean: 143.97, sd: 2.5 },
  2600: { mean: 156.03, sd: 2.93 },
};

const get_adjusted_finish_times = async (rid) => {
  let race = await from_ch_zed_collection({ 4: rid });
  if (_.isEmpty(race)) return [];
  race = struct_race_row_data(race);
  let finishtimes = _.map(race, "finishtime");
  let mean_finishtime = _.mean(finishtimes);
  let dist = race[0].distance;
  let pre = global[dist];
  let mean_diff_from_global = mean_finishtime - pre.mean;
  let diff_allowed = pre.sd;
  let diff_sd_factor = mean_diff_from_global / diff_allowed;
  race = race.map((r) => {
    let adjfinishtime = r.finishtime - diff_sd_factor * diff_allowed;
    return { ...r, adjfinishtime };
  });
  race = _.chain(race).keyBy("hid").mapValues("adjfinishtime").value();
  // console.log({dist,pre,mean_diff_from_global,diff_allowed,diff_sd_factor});
  // let p_race = race;
  // p_race = _.map(race, (r) => {
  //   let { hid, place, finishtime, adjfinishtime } = r;
  //   return { hid, place, finishtime, adjfinishtime };
  // });
  // p_race = _.sortBy(p_race, (a) => parseFloat(a.place));
  // console.table(p_race);
  return race;
};

module.exports = {
  get_adjusted_finish_times,
};
