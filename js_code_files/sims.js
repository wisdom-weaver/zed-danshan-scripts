const appRootPath = require("app-root-path");
const _ = require("lodash");
const { get_races_of_hid } = require("./cyclic_dependency");
const { init, zed_ch } = require("./index-run");
const norminv = require("./norminv");
const { fetch_r, write_to_path } = require("./utils");

const sim_n = 2000;

let preset_global = {
  "0_1000": { mean: 57.74942398611325, sd: 0.7496875127154612 },
  "0_1200": { mean: 71.56243851248803, sd: 0.9292477424270321 },
  "0_1400": { mean: 83.66888777603339, sd: 1.0858509495331343 },
  "0_1600": { mean: 95.87409920822284, sd: 1.1969357408876844 },
  "0_1800": { mean: 107.99367925741764, sd: 1.3705985465642245 },
  "0_2000": { mean: 120.10996633235573, sd: 1.6205793548955292 },
  "0_2200": { mean: 132.2499038971479, sd: 1.7826398905043719 },
  "0_2400": { mean: 144.2151288099578, sd: 2.0510727807958955 },
  "0_2600": { mean: 156.20407922779907, sd: 2.3546380818411454 },
  "1_1000": { mean: 57.501754208319845, sd: 0.9592520600845178 },
  "1_1200": { mean: 71.13870961887463, sd: 1.1750057607182764 },
  "1_1400": { mean: 83.35441543239087, sd: 1.3967229719934073 },
  "1_1600": { mean: 95.45427584910942, sd: 1.6151600317681016 },
  "1_1800": { mean: 107.53266618894146, sd: 1.7572614833488989 },
  "1_2000": { mean: 119.46762200345509, sd: 2.076086268389967 },
  "1_2200": { mean: 131.46959162690152, sd: 2.399287235798542 },
  "1_2400": { mean: 143.5023962281022, sd: 2.7835652894343825 },
  "1_2600": { mean: 155.10938954235988, sd: 3.4315103557717115 },
  "2_1000": { mean: 57.616549127758915, sd: 0.9356763580075484 },
  "2_1200": { mean: 71.2464439098808, sd: 1.1323137107806396 },
  "2_1400": { mean: 83.44948190022798, sd: 1.3632419449885191 },
  "2_1600": { mean: 95.55120037041976, sd: 1.5671882162284272 },
  "2_1800": { mean: 107.58021531670772, sd: 1.7679279522061393 },
  "2_2000": { mean: 119.66639781278565, sd: 1.9556884384603623 },
  "2_2200": { mean: 131.59760706836977, sd: 2.2315801802241273 },
  "2_2400": { mean: 143.64192869728691, sd: 2.6968883360067912 },
  "2_2600": { mean: 155.719043034953, sd: 3.1356433563621424 },
  "3_1000": { mean: 57.64127688870833, sd: 0.8869254540646855 },
  "3_1200": { mean: 71.32533423761883, sd: 1.1144217091725659 },
  "3_1400": { mean: 83.51788937943428, sd: 1.3048384891242832 },
  "3_1600": { mean: 95.65266993533871, sd: 1.5222692152688713 },
  "3_1800": { mean: 107.71761941160133, sd: 1.6769365320366123 },
  "3_2000": { mean: 119.75341276982483, sd: 1.9195435092391036 },
  "3_2200": { mean: 131.76781048693798, sd: 2.2569415784172295 },
  "3_2400": { mean: 143.77685419032585, sd: 2.6246955892059214 },
  "3_2600": { mean: 155.724860402846, sd: 3.0195773668004415 },
  "4_1000": { mean: 57.70289864851273, sd: 0.8286676568870398 },
  "4_1200": { mean: 71.42799943761086, sd: 1.0342269879769037 },
  "4_1400": { mean: 83.62803639009105, sd: 1.2352998441906204 },
  "4_1600": { mean: 95.76134297073058, sd: 1.4218677615748174 },
  "4_1800": { mean: 107.89098702975964, sd: 1.619566036678944 },
  "4_2000": { mean: 119.94617563484125, sd: 1.8170802590138642 },
  "4_2200": { mean: 131.979069559196, sd: 2.0903645333142347 },
  "4_2400": { mean: 144.0248062381801, sd: 2.400066345553495 },
  "4_2600": { mean: NaN, sd: NaN },
  "5_1000": { mean: 57.80239570674755, sd: 0.7779269019917873 },
  "5_1200": { mean: 71.52917766155446, sd: 0.9817066268297848 },
  "5_1400": { mean: 83.7840614030406, sd: 1.1576124069277947 },
  "5_1600": { mean: 95.94628039264303, sd: 1.3467428623126603 },
  "5_1800": { mean: 108.09229549213855, sd: 1.492447362719842 },
  "5_2000": { mean: 120.18273783985971, sd: 1.6739905048647554 },
  "5_2200": { mean: 132.274584949402, sd: 1.9420745036828917 },
  "5_2400": { mean: 144.30536884856988, sd: 2.2377585083183695 },
  "5_2600": { mean: 156.50123934877146, sd: 2.5238879910872285 },
};

const calc_sd = (d_ar) => {
  d_ar = _.compact(d_ar);
  let mean_t = _.mean(d_ar);
  let devs = d_ar.map((e) => e - mean_t);
  devs = devs.map((e) => Math.pow(e, 2));
  let sd = Math.sqrt(_.sum(devs) / devs.length);
  return sd;
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
const get_class_dist_stats = async (hid, c, d) => {
  hid = parseFloat(hid);
  let races = (await get_races_of_hid(hid)) || [];
  // console.log(hid, races.length);
  let d_ar = _.chain(races)
    .filter({ distance: d, thisclass: c })
    .map("adjfinishtime")
    .compact()
    .value();

  // console.log(hid, d_ar.length);
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
  if (_.isEmpty(doc)) return null;
  let { hids, dist, names_ob, race_class, fee, race_name, start_time, status } =
    doc;
  let stats_ob = await Promise.all(
    hids.map((hid) => get_class_dist_stats(hid, race_class, dist))
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
  // console.log("post_process2");
  let { stats_ob, dist: d, race_class: c } = doc;
  let key = `${c}_${d}`;
  stats_ob = stats_ob.map((ea, i) => {
    let { hid, len, st_dev, mean_t } = ea;
    // if (len === 0) return { ...ea, st_dev: null, mean_t: null };
    if (len <= 0 || st_dev == 0 || _.isNaN(st_dev))
      st_dev = preset_global[key].sd;
    else st_dev = preset_global[key].sd * 0.55 + st_dev * 0.45;
    if (mean_t == 0 || _.isNaN(mean_t)) {
      // console.log(hid, mean_t, preset_global[key].mean);
      mean_t = preset_global[key].mean;
    } else mean_t = preset_global[key].mean * 0.55 + mean_t * 0.45;
    return { ...ea, st_dev, mean_t };
  });
  stats_ob = _.compact(stats_ob);
  return { ...doc, stats_ob };
};

const get_simulated_races = ({ stats_ob = [], n = 10 }) => {
  const get_one_sim = (stats_ob) =>
    stats_ob.map(({ mean_t, st_dev }) =>
      mean_t == null
        ? 43951.534978676944
        : norminv(Math.random(), mean_t, st_dev + 1e-14)
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
  // console.log(doc);
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
  let rid = "DZ9eKv9c";
  let a = await get_sims_zed_odds(rid);
  console.log(a);
  console.log(zed_to_real_odds(50));
};
// runner();

module.exports = { get_sims_zed_odds };
