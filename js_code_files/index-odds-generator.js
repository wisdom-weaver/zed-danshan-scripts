const _ = require("lodash");
const prompt = require("prompt-sync")();
const fetch = require("node-fetch");
const mongoose = require("mongoose");
const { init, zed_db, zed_ch, run_func } = require("./index-run");
const { write_to_path, read_from_path } = require("./utils");
const app_root = require("app-root-path");
const {
  get_fee_cat_on,
  download_eth_prices,
  get_at_eth_price_on,
  get_date,
} = require("./base");

let mx = 88433;
// let h = 79917;
let st = 0;
let ed = mx;
// st = h;
// ed = h;
let chunk_size = 25;
let chunk_delay = 100;

const filter_error_horses = (horses = []) => {
  return horses?.filter(({ hid }) => ![15812, 15745].includes(hid));
};

const get_fee_cat = (fee) => {
  fee = parseFloat(fee);
  if (fee == 0) return "G";
  if (fee >= 0.003) return "A";
  if (fee > 0.0015 && fee < 0.003) return "B";
  if (fee <= 0.0015) return "C";
  return undefined;
};

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
];

const from_ch_zed_collection = async (query) => {
  try {
    let data = await zed_ch.db.collection("zed").find(query).toArray();
    data = _.uniqBy(data, (i) => [i["4"], i["6"]].join());
    return data;
  } catch (err) {
    return [];
  }
};

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
  } catch (err) {
    if (data.name == "MongoNetworkError") {
      console.log("MongoNetworkError");
    }
    return [];
  }
  return data;
};
const struct_details_of_hid = (data) => {
  let { bloodline, breed_type, class: thisclass, genotype, hash_info } = data;
  let { color, gender, hex_code, name } = hash_info;
  let { horse_type, owner_stable_slug, rating, win_rate } = data;
  let { parents } = data;
  if (!_.isEmpty(parents)) {
    if (parents.father) parents.father = parents.father.horse_id;
    if (parents.mother) parents.mother = parents.mother.horse_id;
  }
  return {
    name,
    thisclass,
    bloodline,
    breed_type,
    genotype,
    color,
    gender,
    horse_type,
    hex_code,
    owner_stable_slug,
    rating,
    win_rate,
    parents,
  };
};
const get_details_of_hid = async (hid) => {
  try {
    let api = (hid) => `https://api.zed.run/api/v1/horses/get/${hid}`;
    let data = await fetch(api(hid)).then((resp) => resp.json());
    if (_.isEmpty(data) || data.error) return false;
    return struct_details_of_hid(data);
  } catch (err) {
    console.log("Error api.zed on", hid, "refetching....");
    await delay(100);
    return await get_details_of_hid(hid);
  }
};

const get_races_of_hid = async (hid) => {
  if (isNaN(hid)) return [];
  hid = parseInt(hid);
  let query = { 6: hid };
  let data = await from_ch_zed_collection(query);
  data = struct_race_row_data(data);
  data = data.map((e) => {
    let { entryfee: fee, date } = e;
    let fee_cat = get_fee_cat_on({ fee, date });
    return { ...e, fee_cat };
  });
  return data;
};

const filter_acc_to_criteria = ({
  races = [],
  criteria = {},
  extra_criteria = {},
}) => {
  // races = races.filter((ea) => ea.odds != 0);
  if (_.isEmpty(criteria)) return races;
  races = races.filter(
    ({
      distance,
      date,
      entryfee,
      raceid,
      thisclass,
      hid,
      finishtime,
      place,
      name,
      gate,
      odds,
      fee_cat,
    }) => {
      entryfee = parseFloat(entryfee);
      if(odds==0 || odds==null) return false;
      if (criteria?.thisclass !== undefined && criteria?.thisclass !== "#")
        if (!(thisclass == criteria.thisclass)) return false;

      if (criteria?.fee_cat !== undefined && criteria?.fee_cat !== "#")
        if (!(fee_cat == criteria.fee_cat)) return false;

      if (criteria?.distance !== undefined && criteria?.distance !== "####")
        if (!(distance == criteria.distance)) return false;

      if (criteria?.is_paid !== undefined)
        if (criteria?.is_paid == true && entryfee == 0) return false;

      return true;
    }
  );
  if (criteria?.min !== undefined) {
    if (races?.length < criteria.min) {
      // console.log("less", min);
      return [];
    }
  }
  return races;
};

const avg_ar = (ar) => {
  if (_.isEmpty(ar)) return null;
  let sum = 0;
  ar.forEach((e) => (sum += parseFloat(e)));
  return sum / ar.length;
};
const calc_median = (array = []) => {
  if (_.isEmpty(array)) return null;
  array = array.map(parseFloat).sort((a, b) => a - b);
  let median = 0;
  if (array.length == 0) return null;
  if (array.length % 2 === 0) {
    // array with even number elements
    median = (array[array.length / 2] + array[array.length / 2 - 1]) / 2;
  } else {
    median = array[(array.length - 1) / 2]; // array with odd number elements
  }
  // console.log(array, median)
  return median;
};

const cls = ["#", 0, 1, 2, 3, 4, 5];
const dists = ["####", 1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600];
const fee_cats = ["#", "A", "B", "C", "F"];

const get_rated_type = (cf = null) => {
  if (!cf || cf == "na") return "NR";
  if (cf?.endsWith("_")) return "CH";
  if (parseInt(cf[0]) != 0) return "GH";
  return "NR";
};

const get_keys_map = ({ cls, dists, fee_cats }) => {
  let odds_modes = [];
  for (let c of cls)
    for (let f of fee_cats)
      for (let d of dists) odds_modes.push(`${c}${f}${d}`);
  odds_modes = _.uniq(odds_modes);
  return odds_modes;
};
const get_odds_map = ({
  cls,
  dists,
  fee_cats,
  races = [],
  extra_criteria = {},
}) => {
  let odds_modes = get_keys_map({ cls, dists, fee_cats });
  let ob = {};
  odds_modes.forEach((o) => {
    let c = o[0];
    let f = o[1];
    let d = o.slice(2);

    let fr = filter_acc_to_criteria({
      races,
      criteria: { thisclass: c, fee_cat: f, distance: d, ...extra_criteria },
    });
    let odds_ar = Object.values(_.mapValues(fr, "odds"));
    ob[o] = odds_ar;
  });
  return ob;
};

const gen_odds_coll = ({ coll, races = [], extra_criteria = {} }) => {
  let odds_map = get_odds_map({ cls, dists, fee_cats, races, extra_criteria });
  let ob = {};
  // console.log(odds_map);
  for (let key in odds_map) {
    ob[key] = calc_median(odds_map[key]);
  }
  return ob;
};
const upload_odds_coll = async ({ hid, coll, odds_coll }) => {
  try {
    let db_date = new Date().toISOString();
    let ob = { hid, db_date, [coll]: odds_coll };

    await zed_db.db
      .collection(coll)
      .updateOne({ hid }, { $set: ob }, { upsert: true });
  } catch (err) {
    console.log("error gen_odds_coll", hid);
  }
};
const gen_and_upload_odds_coll = async ({
  hid,
  races,
  coll,
  extra_criteria,
}) => {
  try {
    let odds_coll = gen_odds_coll({ races, coll, extra_criteria });
    await upload_odds_coll({ coll, odds_coll, hid });
    // console.log("# hid:", hid, "len", races.length, coll, "done..");
    return odds_coll;
  } catch (err) {
    console.log("error gen_and_upload_odds_coll", hid, coll);
  }
};

const null_hr_ob = { cf: "na", d: null, med: null, side: "-" };

const get_class_hr = async (hid) => {
  hid = parseFloat(hid).toFixed(0);
  // console.log("get_class_hr");
  let races = await get_races_of_hid(hid);
  let odds_ob = gen_odds_coll({ races, extra_criteria: { is_paid: true } });

  let keys = [1, 2, 3, 4, 5].map((ea) => `${ea}#####`);
  let req = keys.map((k) => ({ k, v: odds_ob[k] }));
  // console.log(odds_ob);
  let min_ob = _.minBy(req, "v");
  if (_.isEmpty(min_ob)) return null;
  let res = {
    cf: min_ob.k.slice(0, 1) + "_",
    d: "____",
    med: min_ob.v,
  };
  // console.log(res);
  if (_.isEmpty(res)) return null_hr_ob;
  if (res.med > 10.8) res.cf = "5_";
  return res;
};

const calc_blood_hr = async ({
  odds_live,
  hid,
  tc,
  races_n = 0,
  override_dist = false,
}) => {
  if (
    _.isEmpty(odds_live) ||
    _.isEmpty(_.compact(_.values(odds_live))) ||
    races_n == 0
  ) {
    let class_hr = {};
    if (override_dist == false) class_hr = await get_class_hr(hid);
    if (_.isEmpty(class_hr)) return { ...null_hr_ob, tc };
    else {
      let side = get_side_of_horse({ ...class_hr, tc });
      return { ...class_hr, tc, side };
    }
    return { ...null_hr_ob, tc };
  }

  let keys = get_keys_map({
    cls: cls.slice(2),
    fee_cats: fee_cats.slice(1),
    dists: override_dist != false ? [override_dist] : dists.slice(1),
  });
  let ol = keys.map((k) => ({
    k,
    cf: k.slice(0, 2),
    d: k.slice(2),
    med: odds_live[k],
  }));
  // console.log(ol);
  let hr = {};
  for (let { k, cf, d, med } of ol)
    if (med != null && med <= 10.8) {
      let rb = { cf, d, med };
      let obs = _.filter(ol, (elem) => elem.cf == rb.cf);
      rb = _.minBy(obs, "med");
      hr = { cf: rb.cf, d: rb.d, med: rb.med };
      break;
    }
  if (!_.isEmpty(hr)) {
    hr = { ...hr, tc };
    let side = get_side_of_horse(hr);
    hr = { ...hr, side };
    return hr;
  }
  // console.log(hr);
  // console.log(hr);
  let mm = _.minBy(ol, "med");
  if (!mm || _.isEmpty(mm)) {
    let class_hr = {};
    if (override_dist == false) class_hr = await get_class_hr(hid);
    if (_.isEmpty(class_hr)) return { ...null_hr_ob, tc };
    else {
      let side = get_side_of_horse({ ...class_hr, tc });
      return { ...class_hr, tc, side };
    }
    return { ...null_hr_ob, tc };
  } else {
    let min_ob = { cf: "5C", d: mm?.d, tc, med: mm?.med || null };
    let side = get_side_of_horse(min_ob);
    min_ob = { ...min_ob, side };
    return min_ob;
  }
  return { ...null_hr_ob, tc };
};

const gen_and_upload_blood_hr = async ({
  hid,
  odds_live,
  details,
  races_n = 0,
}) => {
  let tc = details?.thisclass;
  let rating_blood = await calc_blood_hr({ hid, odds_live, races_n, tc });
  let name = details?.name;
  let rated_type = get_rated_type(rating_blood?.cf);
  rating_blood = { ...rating_blood, rated_type };
  let db_date = new Date().toISOString();
  let ob = {
    hid,
    name,
    db_date,
    rating_blood,
    details,
  };
  await zed_db.db
    .collection("rating_blood")
    .updateOne({ hid: parseInt(hid) }, { $set: ob }, { upsert: true });
  // console.log(`# hid:`, hid, `rating_blood:`, rating_blood);
  return rating_blood;
};

const get_side_of_horse = (ea) => {
  // console.log("EA=>", ea);
  let { tc, cf, med } = ea;
  let rc = parseInt(cf[0]);
  // console.log(tc, rc);
  med = parseFloat(med);
  let side = "";
  if (med > 10.8) side = "A";
  else {
    if (tc < rc) side = "A";
    else if (tc > rc) side = "B";
    else if (tc == rc) side = "C";
    else side = "-";
  }
  if (tc == 1 && side == "C") side = "B";
  return side;
};

const generate_blood_mapping = async () => {
  try {
    console.log("generate_blood_mapping");
    // zed_db.db.collection("blood").insert({ id: "blood" });
    // return;
    let def_ar = await zed_db.db.collection("rating_blood").find({}).toArray();
    console.log("len: ", def_ar.length);
    def_ar = filter_error_horses(def_ar);

    let ar = def_ar.map(({ hid, rating_blood }) => {
      if (_.isEmpty(rating_blood)) return null;
      return { hid, rc: parseInt(rating_blood.cf[0]), ...rating_blood };
    });

    ar = _.compact(ar);
    ar = _.sortBy(ar, "cf");
    ar = _.groupBy(ar, "cf");
    ar = _.values(ar).map((e) => _.sortBy(e, "med"));
    ar = _.flatten(ar);

    let ar_GH = ar.filter((ea) => ea.rated_type == "GH");
    ar_GH = ar_GH.map((ea, i) => ({ ...ea, rank: i + 1 }));
    let ar_CH = ar.filter((ea) => ea.rated_type == "CH");
    ar_CH = ar_CH.map((ea) => ({ ...ea, rank: null }));
    let ar_NR = ar.filter((ea) => ea.rated_type == "NR");
    ar_NR = ar_NR.map((ea) => ({ ...ea, rank: null }));
    ar = [...ar_GH, ...ar_CH, ...ar_NR].map(
      ({ hid, rank, rc, tc, cf, d, med, side, rated_type }, i) => {
        return {
          rank,
          hid,
          details: _.find(def_ar, { hid }).details,
          rating_blood: { tc, cf, d, med, side, rc, rated_type },
        };
      }
    );
    // console.log(ar);
    let db_date = new Date().toISOString();
    let i = 1;
    for (let chunk of _.chunk(ar, 10000)) {
      let id = `blood_${i}`;
      await zed_db.db
        .collection("blood")
        .updateOne(
          { id },
          { $set: { id, db_date, blood: chunk } },
          { upsert: true }
        );
      i++;
      console.log("wrote", id);
    }
    await zed_db.db
      .collection("blood")
      .updateOne(
        { id: `blood` },
        { $set: { id: "blood", db_date, len: i } },
        { upsert: true }
      );
    write_to_path({
      file_path: `${app_root}/data/blood/blood.json`,
      data: { id: "blood", db_date, blood: ar },
    });
    await delay(5000);
    try {
      console.log("caching on heroku server");
      await fetch(`https://bs-zed-backend-api.herokuapp.com/blood/download`);
    } catch (err) {}
    return;
  } catch (err) {
    console.log("ERROR generate_blood_mapping\n", err);
  }
};

const generate_odds_for = async (hid) => {
  try {
    let details = await get_details_of_hid(hid);
    if (_.isEmpty(details)) return console.log("# hid:", hid, "empty_horse");
    hid = parseInt(hid);
    if (isNaN(hid)) return;
    let races = await get_races_of_hid(hid);
    // console.log(races[0]);
    // console.log(races.reduce((acc, ea) => (ea.odds == 0 ? acc + 1 : acc), 0));
    // console.log(races);
    // gen_and_upload_odds_overall({ hid, races });
    let or_map = [
      { coll: "odds_overall", extra_criteria: {} },
      { coll: "odds_live", extra_criteria: { is_paid: true, min: 3 } },
    ];
    let odds_overall = await gen_and_upload_odds_coll({
      hid,
      races,
      ...or_map[0],
    });
    // console.log(odds_overall);
    let odds_live = await gen_and_upload_odds_coll({
      hid,
      races,
      ...or_map[1],
    });
    // console.log(or_map[1]);
    let races_n = races?.length || 0;
    let bhr = await gen_and_upload_blood_hr({
      hid,
      odds_live,
      details,
      races_n,
    });
    // console.log(bhr);
    // console.log(`# hid:`, hid, "len:", races.length, "rating_blood:", bhr);
    await get_parent_details_upload(hid);
  } catch (err) {
    console.log("ERROR generate_odds_for", hid);
  }
};

const fetch_all_horses = async () => {
  try {
    let hids = new Array(ed - st + 1).fill(0).map((ea, idx) => st + idx);
    console.log("=> STARTED odds_generator: ", `${st}:${ed}`);

    let i = 0;
    for (let chunk of _.chunk(hids, chunk_size)) {
      i += chunk_size;
      // console.log("\n=> fetching together:", chunk.toString());
      await Promise.all(chunk.map((hid) => generate_odds_for(hid)));
      await delay(chunk_delay);
      console.log("! got", chunk[0], " -> ", chunk[chunk.length - 1]);
      // if (i % 10000 == 0) generate_blood_mapping();
    }
  } catch (err) {
    console.log("ERROR fetch_all_horses\n", err);
  }
};

const start = async () => {
  try {
    await download_eth_prices();

    await fetch_all_horses();

    console.log("## Fetch completed");

    console.log("## Generating Blood Ranks");
    await generate_blood_mapping();
    await give_ranks_on_rating_blood();
    console.log("## Completed Blood Ranks");
    // await delay(60000);
    // try {
    //   console.log("caching live odds on heroku server");
    //   await fetch(
    //     `https://bs-zed-backend-api.herokuapp.com/live/download2?mx=${mx}`
    //   );
    // } catch (err) {}
    // await delay(240000);
    console.log("## DONE");
    return 0;
  } catch (err) {
    console.log("ERROR start-fn \n", err);
  }
};

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const give_rank_hid = async ({ hid, ar }) => {
  try {
    let rank = _.find(ar, { hid })?.rank || null;
    await zed_db.db
      .collection("rating_blood")
      .updateOne({ hid }, { $set: { rank } }, { upsert: true });
  } catch (err) {
    console.log("err give_rank_hid", hid, "\n", err);
  }
};

const give_ranks_on_rating_blood = async () => {
  try {
    await init();
    let obs = await zed_db.db.collection("blood").find().toArray();
    console.log("blood got", obs.length);
    let ar = obs.map((ea) => ea?.blood || []);
    ar = _.flatten(ar);

    // write_to_path({ file_path: `${app_root}/backup/blood.json`, data: ar });
    // console.log("blood wrote to json");

    // let ar = read_from_path({ file_path: `${app_root}/backup/blood.json` });
    let hids = new Array(mx + 1).fill(0).map((ea, i) => i);
    let cs = 50;
    let i = 0;
    for (let chunk of _.chunk(hids, cs)) {
      i++;
      await Promise.all(chunk.map((hid) => give_rank_hid({ hid, ar })));
      if (i % 20 == 0) console.log("done ranks till", chunk[chunk.length - 1]);
    }
    console.log("# completed giving ranks");
  } catch (err) {
    console.log("err giving ranks back\n", err);
  }
};

let odds_generator = async () => {
  try {
    await start();
    // console.log("\n\n\n## Looping... \n\n\n");
    // return await odds_generator();
  } catch (err) {
    console.log("ERROR odds_generator\n", err);
    await delay(5000);
    // return await odds_generator();
  }
};

let run_odds_generator = async () => {
  await init();
  await odds_generator();
  // zed_db.close();
  // zed_ch.close();
};

let run_blood_generator = async () => {
  await init();
  console.log("## Generating Blood Ranks");
  await generate_blood_mapping();
  await give_ranks_on_rating_blood();
  console.log("## Completed Blood Ranks");
  // zed_db.close();
  // zed_ch.close();
};

const run_cache_on_heroku = async () => {
  try {
    console.log("caching on heroku server");
    await fetch(`https://bs-zed-backend-api.herokuapp.com/blood/download`);
  } catch (err) {}
  // await delay(60000);
  // try {
  //   console.log("caching live odds on heroku server");
  //   await fetch(
  //     `https://bs-zed-backend-api.herokuapp.com/live/download2?mx=${mx}`
  //   );
  // } catch (err) {}
  console.log("the rest will be done on the server");
};
const get_parent_details_upload = async (hid) => {
  hid = parseInt(hid);
  let parents_d = await get_parent_details(hid);
  if (!_.isEmpty(parents_d))
    await zed_db
      .collection("rating_blood")
      .updateOne(
        { hid },
        { $set: { "details.parents_d": parents_d } },
        { upsert: true }
      );
};
const get_parent_details = async (hid) => {
  hid = parseInt(hid);
  let doc = await zed_db.collection("rating_blood").findOne({ hid });
  if (_.isEmpty(doc)) return;
  if (_.isEmpty(doc.details.parents)) {
    await delay(500);
    doc = await zed_db.collection("rating_blood").findOne({ hid });
  }
  // console.log(doc)
  if (_.isEmpty(doc)) return;

  let { mother, father } = doc.details.parents;
  let parents_d = { mother: null, father: null };
  // console.log({ mother, father });
  if (mother) parents_d.mother = await get_details_complete_str(mother);
  if (father) parents_d.father = await get_details_complete_str(father);
  return parents_d;
};
const get_details_complete_str = async (hid) => {
  if (!hid) return null;
  hid = parseInt(hid);
  let doc = await zed_db.collection("rating_blood").findOne({ hid });
  // console.log("get_details_complete_str", hid, doc);
  if (_.isEmpty(doc)) return null;
  let { cf, d, med, tc, side, rated_type } = doc.rating_blood;
  let {
    name,
    bloodline,
    breed_type,
    genotype,
    horse_type,
    hex_code,
    owner_stable_slug,
    rating,
    win_rate,
  } = doc.details;

  let str = "";
  let ar = [
    doc.hid,
    name,
    rating,
    win_rate,
    bloodline,
    breed_type,
    genotype,
    horse_type,
    hex_code,
    owner_stable_slug,
    rated_type,
    cf,
    d,
    med,
    side,
    tc,
    doc.rank,
  ];

  str = ar.join("|+|");
  // console.log(str);
  return str;
};

module.exports = {
  calc_blood_hr,
  get_races_of_hid,
  odds_generator,
  run_odds_generator,
  get_details_of_hid,
  from_ch_zed_collection,
  generate_blood_mapping,
  run_blood_generator,
  mx,
  get_rated_type,
  give_ranks_on_rating_blood,
  run_cache_on_heroku,
  struct_race_row_data,
};
