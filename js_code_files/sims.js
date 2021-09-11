const appRootPath = require("app-root-path");
const _ = require("lodash");
const norminv = require("./norminv");
const { fetch_r, write_to_path } = require("./utils");

const sim_n = 2000;

const preset_SDs = {
  1000: 0.86,
  1200: 1.08,
  1400: 1.29,
  1600: 1.47,
  1800: 1.65,
  2000: 1.88,
  2200: 2.16,
  2400: 2.51,
  2600: 2.95,
};
const presset_MEANs = {
  1000: 57.72,
  1200: 71.41,
  1400: 83.61,
  1600: 95.77,
  1800: 107.87,
  2000: 119.9,
  2200: 131.97,
  2400: 143.97,
  2600: 156.02,
};

const post_process2 = (doc) => {
  let { stats_ob, dist: d } = doc;
  stats_ob = stats_ob.map((ea, i) => {
    let { hid, len, st_dev, mean_t } = ea;
    if (len === 0) return { ...ea, st_dev: null, mean_t: null };
    if (len <= 0 || st_dev == 0) st_dev = preset_SDs[d];
    else st_dev = preset_SDs[d] * 0.55 + st_dev * 0.45;
    if (mean_t == 0) mean_t = preset_SDs[d];
    else mean_t = presset_MEANs[d] * 0.55 + mean_t * 0.45;
    return { ...ea, st_dev, mean_t };
  });
  stats_ob = _.compact(stats_ob);
  return { ...doc, stats_ob };
};

const get_simulated_races = ({ stats_ob = [], n = 10 }) => {
  const get_one_sim = (stats_ob) =>
    stats_ob.map(({ mean_t, st_dev }) =>
      mean_t == null ? 999 : norminv(Math.random(), mean_t, st_dev + 1e-14)
    );
  let sims = new Array(n).fill(0).map(() => {
    let sim_ar = get_one_sim(stats_ob);
    let sim_ar_sort = [...sim_ar].sort((a, b) => a - b);
    let [s1, s2, s3] = sim_ar_sort;
    let i1 = sim_ar.findIndex((e) => e == s1);
    let i2 = sim_ar.findIndex((e) => e == s2);
    let i3 = sim_ar.findIndex((e) => e == s3);
    let h1 = stats_ob[i1].hid;
    let h2 = stats_ob[i2].hid;
    let h3 = stats_ob[i3].hid;
    let places = [
      [i1, h1, s1],
      [i2, h2, s2],
      [i3, h3, s3],
    ];
    return { sim_ar, places };
  });
  // console.log(sims);
  return sims;
};

const get_results_from_sims = ({ sims, stats_ob, n }) => {
  let ar = new Array(12).fill({}).map((ea) => ({ p1: 0, p2: 0, p3: 0 }));
  _.map(sims, "places").map((sim_p) => {
    let [[i1, h1, s1], [i2, h2, s2], [i3, h3, s3]] = sim_p;
    // console.log(i1,i2,i3);
    ar[i1].p1 += 1;
    ar[i2].p2 += 1;
    ar[i3].p3 += 1;
  });
  ar = ar.map(({ p1, p2, p3 }) => {
    p1 = p1 == 0 ? 999 : (p1 = 1 / (p1 / n) - 1);
    p2 = p2 == 0 ? 999 : (p2 = 1 / (p2 / n) - 1);
    p3 = p3 == 0 ? 999 : (p3 = 1 / (p3 / n) - 1);
    let tot = p1 + p2 + p3;
    return { p1, p2, p3, tot };
  });
  // console.log(ar)

  let ar_tots = _.map(ar, "tot");
  let flames_order = [...ar_tots].sort((a, b) => a - b);
  let [f1, f2, f3] = flames_order;
  let i1 = ar_tots.findIndex((e) => e == f1);
  let i2 = ar_tots.findIndex((e) => e == f2);
  let i3 = ar_tots.findIndex((e) => e == f3);
  let result_disp = stats_ob.map((stat_ob, i) => {
    let { hid, name } = stat_ob;
    let { p1: odds, tot } = ar[i];
    let flame = 0;
    if ([f1, f2, f3].includes(tot)) flame = 1;
    let odds_real = odds;
    let odds_zed = real_to_zed_odds(odds_real);
    return { hid, name, odds_real, odds_zed, tot, flame };
  });
  // console.log(result_disp);

  return { result_table: ar, result_disp };
};

const zed_to_real_odds = (odds) => {
  try {
    return 1 / (0.95998 * Math.exp(-0.213 * odds));
  } catch (err) {
    return "";
  }
};

const real_to_zed_odds = (r) => {
  return Math.log(0.95998 * r) / 0.213;
};

const get_sims_result_for_rid = async (rid) => {
  let api = `https://bs-zed-backend-api.herokuapp.com/simulation/race/${rid}`;
  let doc = await fetch_r(api);
  doc = post_process2(doc);
  let n = sim_n;
  let { stats_ob, ...a } = doc;
  let sims = get_simulated_races({ stats_ob, n });

  let result = get_results_from_sims({ sims, stats_ob, n });
  let rand = parseInt(Math.random() * n);
  let rand_sim = sims[rand]?.sim_ar;
  // console.log(rand_sim);
  return { ...a, result, rand_sim };
};

const get_sims_zed_odds = async (rid) => {
  try {
    if (!rid) return {};
    let ob = await get_sims_result_for_rid(rid);
    let ret = {};
    ret = _.chain(ob?.result.result_disp)
      .keyBy("hid")
      .mapValues("odds_zed")
      .value();
    // write_to_path({ file_path: `${appRootPath}/data_files/sims/sims-${Date.now()}-${rid}.json`, data: ob });
    return ret;
  } catch (err) {
    return {};
  }
};

const runner = async () => {
  let rid = "UmUcfXZh";
  let a = await get_sims_zed_odds(rid);
  console.log(a);
};
// runner();

module.exports = { get_sims_zed_odds };
