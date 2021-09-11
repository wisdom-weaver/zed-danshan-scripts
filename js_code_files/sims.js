const appRootPath = require("app-root-path");
const _ = require("lodash");
const { get_races_of_hid } = require("./cyclic_dependency");
const { init } = require("./index-run");
const norminv = require("./norminv");
const { fetch_r, write_to_path } = require("./utils");

const sim_n = 2000;

let preset_global = {
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

const get_hid_doc = async (hid) => {
  let api = `https://api.zed.run/api/v1/horses/get/${hid}`;
  let doc = await fetch_r(api);
  return doc;
};
const get_race_doc = async (rid) => {
  let api = `https://racing-api.zed.run/api/v1/races?race_id=${rid}`;
  let doc = await fetch_r(api);
  if (_.isEmpty(doc) || doc.error) return null;

  let {
    length: dist,
    gates,
    final_positions,
    class: race_class,
    fee,
    name: race_name,
    start_time,
    status,
  } = doc;
  let hids = _.values(gates);

  let names_ob;
  if (_.isEmpty(final_positions)) {
    names_ob = await Promise.all(
      hids.map((h) =>
        get_hid_doc(h).then((d) => [h, d?.hash_info?.name || "-"])
      )
    );
    names_ob = _.fromPairs(names_ob);
  } else
    names_ob = _.chain(final_positions)
      .keyBy("horse_id")
      .mapValues("name")
      .value();

  return {
    dist,
    race_class,
    fee,
    race_name,
    start_time,
    status,
    hids,
    names_ob,
  };
};
const get_all_dist_stats = async (hid) => {
  hid = parseFloat(hid);
  let races = (await get_races_of_hid(hid)) || [];
  // console.table(races);
  let mini = {};
  races.forEach((r) => {
    let { distance, finishtime } = r;
    mini[distance] = [...(mini[distance] || []), finishtime];
  });

  let ds = [1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600];

  let p_ar = [];
  for (let d of ds) {
    let d_ar = mini[d] || [];
    let min_t = _.min(d_ar);
    let max_t = _.max(d_ar);
    let mean_t = _.mean(d_ar);
    let range = max_t - min_t;

    let st_dev = (() => {
      let devs = d_ar.map((e) => e - mean_t);
      devs = devs.map((e) => Math.pow(e, 2));
      return Math.sqrt(_.sum(devs) / devs.length);
    })();

    p_ar.push({
      hid,
      d,
      len: d_ar.length,
      min_t,
      max_t,
      mean_t,
      range,
      st_dev,
    });
  }
  // console.table(pob);
  return p_ar || [];
};
const get_adjusted_finish_times = async (rid) => {
  let race = await from_ch_zed_collection({ 4: rid });
  if (_.isEmpty(race)) return [];
  race = struct_race_row_data(race);
  let finishtimes = _.map(race, "finishtime");
  let mean_finishtime = _.mean(finishtimes);
  let dist = race[0].distance;
  let pre = preset_global[dist];
  let mean_diff_from_global = mean_finishtime - pre.mean;
  let diff_allowed = pre.sd;
  let diff_sd_factor = mean_diff_from_global / diff_allowed;
  race = race.map((r) => {
    let adjfinishtime = r.finishtime - diff_sd_factor * diff_allowed;
    return { ...r, adjfinishtime };
  });

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
const get_dist_stats = async (hid, d) => {
  hid = parseFloat(hid);
  let races = (await get_races_of_hid(hid)) || [];
  // console.log(hid, races.length);
  let d_ar = _.chain(races)
    .filter({ distance: d })
    .map("adjfinishtime")
    .compact()
    .value();

  // console.log(d_ar)
  let min_t = _.min(d_ar);
  let max_t = _.max(d_ar);
  let mean_t = _.mean(d_ar);
  let range = max_t - min_t;

  let st_dev = (() => {
    let devs = d_ar.map((e) => e - mean_t);
    devs = devs.map((e) => Math.pow(e, 2));
    return Math.sqrt(_.sum(devs) / devs.length);
  })();

  return {
    hid,
    d,
    len: d_ar.length,
    min_t,
    max_t,
    mean_t,
    range,
    st_dev,
  };
};
const get_simulation_base_data = async (rid) => {
  if (!rid) return null;
  let doc = await get_race_doc(rid);
  // console.log(doc);
  if (_.isEmpty(doc)) return null;
  let { hids, dist, names_ob, race_class, fee, race_name, start_time, status } =
    doc;
  let stats_ob = await Promise.all(
    hids.map((hid) => get_dist_stats(hid, dist))
  );
  stats_ob = stats_ob.map((a) => ({ ...a, name: names_ob[a.hid] }));
  return {
    rid,
    dist,
    stats_ob,
    race_class,
    fee,
    race_name,
    start_time,
    status,
  };
};

const post_process2 = (doc) => {
  let { stats_ob, dist: d } = doc;
  stats_ob = stats_ob.map((ea, i) => {
    let { hid, len, st_dev, mean_t } = ea;
    if (len === 0) return { ...ea, st_dev: null, mean_t: null };
    if (len <= 0 || st_dev == 0) st_dev = preset_global[d].sd;
    else st_dev = preset_global[d].sd * 0.55 + st_dev * 0.45;
    if (mean_t == 0) mean_t = preset_global[d].sd;
    else mean_t = preset_global[d].mean * 0.55 + mean_t * 0.45;
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
  // let api = `https://bs-zed-backend-api.herokuapp.com/simulation/race/${rid}`;
  // let doc = await fetch_r(api);
  // console.log(rid);
  let doc = await get_simulation_base_data(rid);
  // console.log(doc);
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
    // write_to_path({
    //   file_path: `${appRootPath}/data_files/sims/sims-${Date.now()}-${rid}.json`,
    //   data: ob,
    // });
    return ret;
  } catch (err) {
    return {};
  }
};

const runner = async () => {
  await init();
  let rid = "76UO2GyO";
  let a = await get_sims_zed_odds(rid);
  console.log(a);
};
// runner();

module.exports = { get_sims_zed_odds };
