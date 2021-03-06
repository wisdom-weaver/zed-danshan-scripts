const _ = require("lodash");
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
const { generate_max_horse } = require("./max_horses");
const {
  get_n_upload_rating_flames,
  generate_rating_flames,
  generate_rating_flames_wraces,
} = require("./update_flame_concentration");
const { dec, get_fee_tag } = require("./utils");
const {
  generate_breed_rating,
  generate_breed_rating_m1,
  init_btbtz,
} = require("./horses-kids-blood2");
const {
  generate_rating_blood,
  generate_rating_blood_from_hid,
  generate_rating_blood_dist_for_hid,
  get_blood_str,
  generate_rating_blood_dist,
  generate_rating_blood_both_for_hid,
} = require("./blood_rating_blood-2");
const {
  generate_odds_flames_hid,
  generate_odds_flames,
} = require("./odds-flames-blood2");
const { horse_stats_range, get_horse_stats_raw } = require("./zed_horse_stats");
const cyclic_depedency = require("./cyclic_dependency");

let mx;
let st = 1;
let ed = mx;
let chunk_size = 25;
let chunk_delay = 100;

const initiate = async () => {
  await init();
  await download_eth_prices();
};

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
  ["14", "fee_cat"],
  ["15", "adjfinishtime"],
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
  return cyclic_depedency.get_races_of_hid(hid);
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
      if (odds == 0 || odds == null) return false;
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
const fee_tags = ["#", "A", "B", "C", "D", "E", "F"];

const get_rated_type = (cf = null) => {
  // console.log(cf);
  if (!cf || cf == "na") return "NR";
  if (cf?.endsWith("_")) return "CH";
  if (parseInt(cf[0]) != 0) return "GH";
  return "NR";
};

const get_keys_map = ({ cls, dists, fee_tags }) => {
  let odds_modes = [];
  for (let c of cls)
    for (let f of fee_tags)
      for (let d of dists) odds_modes.push(`${c}${f}${d}`);
  odds_modes = _.uniq(odds_modes);
  return odds_modes;
};
const get_odds_map = ({
  cls,
  dists,
  fee_tags,
  races = [],
  extra_criteria = {},
}) => {
  let odds_modes = get_keys_map({ cls, dists, fee_tags });
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
  let odds_map = get_odds_map({ cls, dists, fee_tags, races, extra_criteria });
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
    return console.log("error gen_odds_coll", hid);
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
      return { ...class_hr, tc };
    }
    return { ...null_hr_ob, tc };
  }

  let keys = get_keys_map({
    cls: cls.slice(2),
    fee_tags: fee_tags.slice(1),
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
      return { ...class_hr, tc };
    }
    return { ...null_hr_ob, tc };
  } else {
    let min_ob = { cf: "5C", d: mm?.d, tc, med: mm?.med || null };
    return min_ob;
  }
  return { ...null_hr_ob, tc };
};
const gen_rating_blood = async (ob) => {
  let a = await calc_blood_hr(ob);
  let hid = ob.hid;
  let rated_type = get_rated_type(a?.cf);
  a = { hid, ...a, rated_type };
  let side = get_side_of_horse(a);
  a = { ...a, side };
  return a;
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
  let { tc, cf, med, rated_type } = ea;
  if (rated_type !== "GH") return "-";
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

const get_flames_str = (ob) => {
  let { key, avg_points, flames_per } = ob;
  if (key == null) return "NF";
  let cf = key.slice(0, 2);
  let str = `${cf}_${flames_per}%_${dec(avg_points, 2)}pt`;
  return str;
};

const generate_odds_for = async (hid) => {
  try {
    hid = parseInt(hid);
    if (isNaN(hid)) return;
    let doc = await zed_db.db
      .collection("horse_details")
      .findOne({ hid }, { projection: { _id: 1, tc: 1 } });

    if (_.isEmpty(doc)) {
      console.log("emp horse", hid);
      return null;
    }

    let { tc, name } = doc;
    // let races = await get_races_of_hid(hid);
    let races = await get_races_of_hid(hid);

    let or_map = [
      { coll: "odds_overall", extra_criteria: {} },
      { coll: "odds_live", extra_criteria: { is_paid: true, min: 3 } },
    ];
    let odds_overall = await gen_odds_coll({
      hid,
      races,
      ...or_map[0],
    });
    // console.log(odds_overall);
    let odds_live = await gen_odds_coll({ hid, races, ...or_map[1] });
    // console.log(or_map[1]);
    let races_n = races?.length || 0;

    let rating_blood = await generate_rating_blood({ hid, races, tc });
    rating_blood.name = name;

    let rating_flames = await generate_rating_flames_wraces({ hid, races });
    let odds_flames = await generate_odds_flames({ hid, races });
    let stats = await get_horse_stats_raw({ hid, races });
    let rating_blood_dist = await generate_rating_blood_dist({ hid, races });

    let blood_str = get_blood_str(rating_blood);
    let flames_str = get_flames_str(rating_flames);

    console.log(
      `# hid:`,
      hid,
      "len:",
      races.length,
      blood_str,
      flames_str,
      dec(stats?.avg_paid_fee_usd)
    );
    const h_docs = {
      hid,
      odds_live,
      odds_overall,
      rating_blood,
      rating_flames,
      odds_flames,
      stats,
      rating_blood_dist,
    };
    // console.log(h_docs);
    return h_docs;
  } catch (err) {
    console.log("ERROR generate_odds_for", hid);
    console.log("ERROR generate_odds_for", hid, err);
    return null;
  }
};

const odds_generator_bulk_push = async (obar) => {
  let odds_live_bulk = [];
  let odds_overall2_bulk = [];
  let rating_blood2_bulk = [];
  let rating_flames2_bulk = [];
  let odds_flames2_bulk = [];
  let stats_bulk = [];
  let rating_blood_dist_bulk = [];
  for (let ob of obar) {
    if (_.isEmpty(ob)) continue;
    let {
      hid,
      odds_live,
      odds_overall,
      rating_blood,
      rating_flames,
      odds_flames,
      stats,
      rating_blood_dist,
    } = ob;
    odds_live_bulk.push({
      updateOne: {
        filter: { hid },
        update: { $set: { hid, odds_live } },
        upsert: true,
      },
    });
    odds_overall2_bulk.push({
      updateOne: {
        filter: { hid },
        update: { $set: { hid, odds_overall } },
        upsert: true,
      },
    });
    rating_blood2_bulk.push({
      updateOne: {
        filter: { hid },
        update: { $set: rating_blood },
        upsert: true,
      },
    });
    rating_flames2_bulk.push({
      updateOne: {
        filter: { hid },
        update: { $set: rating_flames },
        upsert: true,
      },
    });
    odds_flames2_bulk.push({
      updateOne: {
        filter: { hid },
        update: { $set: odds_flames },
        upsert: true,
      },
    });
    stats_bulk.push({
      updateOne: {
        filter: { hid },
        update: { $set: stats },
        upsert: true,
      },
    });
    rating_blood_dist_bulk.push({
      updateOne: {
        filter: { hid },
        update: { $set: rating_blood_dist },
        upsert: true,
      },
    });
  }
  await zed_db.db.collection("odds_live").bulkWrite(odds_live_bulk);
  await zed_db.db.collection("odds_overall2").bulkWrite(odds_overall2_bulk);
  await zed_db.db.collection("rating_blood2").bulkWrite(rating_blood2_bulk);
  await zed_db.db.collection("rating_flames2").bulkWrite(rating_flames2_bulk);
  await zed_db.db.collection("odds_flames2").bulkWrite(odds_flames2_bulk);
  await zed_db.db.collection("rating_blood_dist").bulkWrite(rating_blood_dist_bulk);
  await zed_db.db.collection("horse_stats").bulkWrite(stats_bulk);
  console.log("wrote bulk", obar.length);
};
const breed_generator_bulk_push = async (obar) => {
  try {
    if (_.isEmpty(obar)) return;
    let rating_breed2_bulk = [];
    for (let ob of obar) {
      if (_.isEmpty(ob)) continue;
      let { hid } = ob;
      rating_breed2_bulk.push({
        updateOne: {
          filter: { hid },
          update: { $set: ob },
          upsert: true,
        },
      });
    }
    await zed_db.db.collection("rating_breed2").bulkWrite(rating_breed2_bulk);
    console.log(
      "wrote bulk",
      obar.length,
      "..",
      obar[0].hid,
      "->",
      obar[obar.length - 1].hid
    );
  } catch (err) {
    console.log(
      "mongo err",
      obar.length,
      "..",
      obar[0].hid,
      "->",
      obar[obar.length - 1].hid
    );
  }
};

const general_bulk_push = async (coll, obar) => {
  try {
    if (_.isEmpty(obar)) return;
    let bulk = [];
    for (let ob of obar) {
      if (_.isEmpty(ob)) continue;
      let { hid } = ob;
      bulk.push({
        updateOne: {
          filter: { hid },
          update: { $set: ob },
          upsert: true,
        },
      });
    }
    await zed_db.db.collection(coll).bulkWrite(bulk);
    let len = obar.length;
    let sth = obar[0].hid;
    let edh = obar[obar.length - 1].hid;
    console.log("wrote bulk", coll, len, "..", sth, "->", edh);
  } catch (err) {
    console.log("err mongo bulk", coll, coll, obar && obar[0]?.hid);
  }
};

const odds_generator_all_horses = async () => {
  try {
    await initiate();
    await init_btbtz();
    let st = 1;
    let ed = 400000;
    let cs = 500;
    let hids = new Array(ed - st + 1).fill(0).map((ea, idx) => st + idx);
    // let hids = [77052, 78991, 80769, 87790, 88329, 88427];

    outer: while (true) {
      console.log("=> STARTED odds_generator: ", `${st}:${ed}`);
      for (let chunk of _.chunk(hids, cs)) {
        // console.log("\n=> fetching together:", chunk.toString());
        let obar = await Promise.all(
          chunk.map((hid) => generate_odds_for(hid))
        );
        obar = _.compact(obar);
        if (obar.length == 0) {
          console.log("starting from initial");
          continue outer;
        }
        // console.table(obar);
        try {
          await odds_generator_bulk_push(obar);
        } catch (err) {
          console.log("mongo err");
        }
        console.log("! got", chunk[0], " -> ", chunk[chunk.length - 1]);
        await delay(chunk_delay);
      }
    }
  } catch (err) {
    console.log("ERROR fetch_all_horses\n", err);
  }
};
const blood_generator_all_horses = async (st, ed) => {
  try {
    await initiate();
    await init_btbtz();
    if (!st) st = 1;
    else st = parseFloat(st);
    if (!ed) ed = 400000;
    else ed = parseFloat(ed);
    let cs = 20;
    let hids = new Array(ed - st + 1).fill(0).map((ea, idx) => st + idx);
    // let hids = [26646, 21744, 21512];
    // let hids = [21888];
    // outer: while (true) {
    console.log("=> STARTED blood_generator: ", `${st}:${ed}`);
    for (let chunk of _.chunk(hids, cs)) {
      // console.log("\n=> fetching together:", chunk.toString());
      let obar = await Promise.all(
        chunk.map((hid) => generate_rating_blood_from_hid(hid))
      );
      obar = _.compact(obar);
      if (obar.length == 0) {
        // console.log("starting from initial");
        // continue outer;
        console.log("exit");
        return;
      }
      // console.table(obar);
      try {
        await general_bulk_push("rating_blood2", obar);
      } catch (err) {
        console.log("mongo err");
      }
      console.log("! got", chunk[0], " -> ", chunk[chunk.length - 1]);
      await delay(chunk_delay);
    }
    // }
  } catch (err) {
    console.log("ERROR fetch_all_horses\n", err);
  }
};
const blood_generator_dist_all_horses = async (st, ed) => {
  try {
    await initiate();
    await init_btbtz();
    if (!st) st = 1;
    else st = parseFloat(st);
    if (!ed) ed = 400000;
    let cs = 20;
    let hids = new Array(ed - st + 1).fill(0).map((ea, idx) => st + idx);
    // let hids = [26646, 21744, 21512];
    // let hids = [21888];
    // outer: while (true) {
    console.log("=> STARTED blood_generator: ", `${st}:${ed}`);
    for (let chunk of _.chunk(hids, cs)) {
      // console.log("\n=> fetching together:", chunk.toString());
      let obar = await Promise.all(
        chunk.map((hid) => generate_rating_blood_dist_for_hid(hid))
      );
      obar = _.compact(obar);
      // console.log(obar);
      if (obar.length == 0) {
        // console.log("starting from initial");
        // continue outer;
        console.log("exit");
        return;
      }
      // console.table(obar);
      try {
        await general_bulk_push("rating_blood_dist", obar);
      } catch (err) {
        console.log("mongo err");
      }
      console.log("! got", chunk[0], " -> ", chunk[chunk.length - 1]);
      await delay(chunk_delay);
    }
    return;
    // }
  } catch (err) {
    console.log("ERROR fetch_all_horses\n", err);
  }
};
const blood_generator_both_all_horses = async (st, ed) => {
  try {
    await initiate();
    await init_btbtz();
    if (!st) st = 1;
    else st = parseFloat(st);
    if (!ed) ed = 400000;
    let cs = 20;
    let hids = new Array(ed - st + 1).fill(0).map((ea, idx) => st + idx);
    // let hids = [26646, 21744, 21512];
    // let hids = [21888];
    // outer: while (true) {
    console.log("=> STARTED blood_generator_both: ", `${st}:${ed}`);
    for (let chunk of _.chunk(hids, cs)) {
      // console.log("\n=> fetching together:", chunk.toString());
      let obar = await Promise.all(
        chunk.map((hid) => generate_rating_blood_both_for_hid(hid))
      );
      obar = _.compact(obar) || [];
      let overall_ar = _.chain(obar)
        .map((i) => {
          return i.overall || null;
        })
        .compact()
        .value();
      let forleader_ar = _.chain(obar)
        .map((i) => {
          return i.forleader || null;
        })
        .compact()
        .value();
      // console.log(obar);
      if (obar.length == 0) {
        // console.log("starting from initial");
        // continue outer;
        console.log("exit");
        return;
      }
      // console.table(obar);
      try {
        await general_bulk_push("rating_blood2", overall_ar);
        await general_bulk_push("rating_blood_dist", forleader_ar);
      } catch (err) {
        console.log("mongo err");
      }
      console.log("! got", chunk[0], " -> ", chunk[chunk.length - 1]);
      await delay(chunk_delay);
    }
    return;
    // }
  } catch (err) {
    console.log("ERROR fetch_all_horses\n", err);
  }
};

const breed_generator_all_horses = async () => {
  try {
    console.log("breed_generator_all_horses");
    await initiate();
    await init_btbtz();
    let st = 1;
    let ed = 4000000;
    let cs = 500;
    let hids = new Array(ed - st + 1).fill(0).map((ea, idx) => st + idx);
    // hids = [3722];
    outer: while (true) {
      console.log("=> STARTED breed_generator: ", `${st}:${ed}`);
      for (let chunk of _.chunk(hids, cs)) {
        // console.log("\n=> fetching together:", chunk.toString());
        let obar = await Promise.all(
          chunk.map((hid) => generate_breed_rating(hid))
        );
        let emps = _.filter(obar, { kids_n: 0, kid_score: null });
        console.log("emptys len:", emps.length);
        if (emps.length == chunk.length) {
          console.log("starting from initial");
          continue outer;
        }
        // console.table(obar);
        try {
          await breed_generator_bulk_push(obar);
        } catch (err) {
          console.log("mongo err");
        }
        console.log("! got", chunk[0], " -> ", chunk[chunk.length - 1]);
        await delay(chunk_delay);
      }
    }
    console.log("breed_generator_all_horses");
  } catch (err) {
    console.log("ERROR fetch_all_horses\n", err);
  }
};
const breed_generator_m1_all_horses = async () => {
  try {
    console.log("breed_generator_m1_all_horses");
    await initiate();
    await init_btbtz();
    let st = 1;
    let ed = 4000000;
    let cs = 500;
    let hids = new Array(ed - st + 1).fill(0).map((ea, idx) => st + idx);
    // let hids = [26646, 21744, 21512];
    outer: while (true) {
      console.log("=> STARTED breed_generator: ", `${st}:${ed}`);
      for (let chunk of _.chunk(hids, cs)) {
        // console.log("\n=> fetching together:", chunk.toString());
        let obar = await Promise.all(
          chunk.map((hid) => generate_breed_rating_m1(hid))
        );
        let emps = _.filter(obar, { kids_n: 0, kid_score: null });
        console.log("emptys len:", emps.length);
        if (emps.length == chunk.length) {
          console.log("starting from initial");
          continue outer;
        }
        // console.table(obar);
        try {
          await general_bulk_push("rating_breed_m1", obar);
        } catch (err) {
          console.log("mongo err");
        }
        console.log("! got", chunk[0], " -> ", chunk[chunk.length - 1]);
        await delay(chunk_delay);
      }
    }
    console.log("breed_generator_m1_all_horses");
  } catch (err) {
    console.log("ERROR fetch_all_horses\n", err);
  }
};

const odds_flames_generator_all_horses = async () => {
  try {
    console.log("breed_generator_all_horses");
    await initiate();
    let st = 1;
    let ed = 4000000;
    let cs = 500;
    let hids = new Array(ed - st + 1).fill(0).map((ea, idx) => st + idx);
    // let hids = [3312];
    // outer: while (true) {
    console.log("=> STARTED odds_flames_generator: ", `${st}:${ed}`);
    for (let chunk of _.chunk(hids, cs)) {
      // console.log("\n=> fetching together:", chunk.toString());
      let obar = await Promise.all(
        chunk.map((hid) => generate_odds_flames_hid(hid))
      );
      let emps = _.filter(obar, { races_n: 0 });
      console.log("emptys len:", emps.length);
      if (emps.length == chunk.length) {
        console.log("reached end");
        // continue outer;
        break;
      }
      // console.table(obar);
      try {
        await general_bulk_push("odds_flames2", obar);
      } catch (err) {
        console.log("mongo err");
      }
      console.log("! got", chunk[0], " -> ", chunk[chunk.length - 1]);
      await delay(chunk_delay);
    }
    // }
    console.log("odds_flames_generator_all_horses completed");
  } catch (err) {
    console.log("ERROR fetch_all_horses\n", err);
  }
};

const breed_generator_errs = async () => {
  await init();
  let docs1 =
    (await zed_db.db
      .collection("rating_breed2")
      .find({ br: { $ne: null, $lt: 0 } }, { projection: { hid: 1, _id: 0 } })
      .toArray()) || [];
  let docs2 =
    (await zed_db.db
      .collection("rating_breed2")
      .find({ br: { $ne: null, $gt: 3 } }, { projection: { hid: 1, _id: 0 } })
      .toArray()) || [];
  let hids = [..._.map(docs1, "hid"), ..._.map(docs2, "hid")];
  console.log("breed errs:", hids.length);
  await breed_generator_for_hids(hids);
  console.log("check again");
  let docs3 =
    (await zed_db.db
      .collection("rating_breed2")
      .find({ br: { $ne: null, $lt: 0 } }, { projection: { hid: 1, _id: 0 } })
      .toArray()) || [];
  let docs4 =
    (await zed_db.db
      .collection("rating_breed2")
      .find({ br: { $ne: null, $gt: 3 } }, { projection: { hid: 1, _id: 0 } })
      .toArray()) || [];
  hids = [..._.map(docs3, "hid"), ..._.map(docs4, "hid")];
  console.log("breed errs:", hids.length);
  await breed_generator_for_hids(hids);
  console.log("done");
  await delay(1000 * 60 * 60);
  return await breed_generator_errs();
};

const odds_generator_for_hids = async (hids) => {
  try {
    let i = 0;
    let cs = 500;
    for (let chunk of _.chunk(hids, cs)) {
      i += chunk_size;
      // console.log("\n=> fetching together:", chunk.toString());
      let obar = await Promise.all(chunk.map((hid) => generate_odds_for(hid)));
      try {
        await odds_generator_bulk_push(obar);
      } catch (err) {
        console.log("mongo err");
      }
      console.log("! got", chunk[0], " -> ", chunk[chunk.length - 1]);
      await delay(chunk_delay);
    }
  } catch (err) {
    console.log("ERROR fetch_for_hids\n", err);
  }
};

const breed_generator_for_hids = async (hids) => {
  try {
    await init_btbtz();
    let i = 0;
    for (let chunk of _.chunk(hids, 50)) {
      i += chunk_size;
      // console.log("\n=> fetching together:", chunk.toString());
      let obar = await Promise.all(
        chunk.map((hid) => generate_breed_rating(hid))
      );
      // console.table(obar);
      try {
        await breed_generator_bulk_push(obar);
      } catch (err) {
        console.log("mongo err");
      }
      console.log("! got", chunk[0], " -> ", chunk[chunk.length - 1]);
      await delay(chunk_delay);
    }
  } catch (err) {
    console.log("ERROR odds_generator_for_hids\n", err);
  }
};

const clone_odds_overall = async () => {
  let st = 1;
  let ed = 82000;
  let cs = 500;
  let hids = new Array(ed - st + 1).fill(0).map((e, i) => i + st);
  // let hids = [31896];
  for (let chunk_hids of _.chunk(hids, cs)) {
    let obar = await zed_db.db
      .collection("odds_overall")
      .find(
        { hid: { $in: hids } },
        { projection: { _id: 0, hid: 1, odds_overall: 1 } }
      )
      .toArray();
    let mpush = [];
    mpush = obar.map((ob) => {
      return {
        updateOne: {
          filter: { hid: ob.hid },
          update: { $set: ob },
          upsert: true,
        },
      };
    });
    if (!_.isEmpty(mpush))
      await zed_db.db.collection("odds_overall2").bulkWrite(mpush);
    console.log("done", chunk_hids.toString());
  }
};

const update_odds_and_breed_for_race_horses = async (horses_tc_ob) => {
  // console.log(horses_tc_ob);
  let hids = _.chain(horses_tc_ob)
    .keys()
    .map((i) => parseInt(i))
    .value();
  let parents_docs = await zed_db.db
    .collection("horse_details")
    .find(
      { hid: { $in: hids } },
      { projection: { hid: 1, parents: 1, _id: 0 } }
    )
    .toArray();
  let parents_hids = [];
  parents_docs.forEach((e) => {
    let { parents = {} } = e;
    parents = _.values(parents) || [];
    parents_hids = [...parents_hids, ...parents];
  });
  parents_hids = _.compact(parents_hids);
  console.log(hids);
  console.log(parents_hids);

  let tc_bulk = [];
  for (let [hid, tc] of _.entries(horses_tc_ob)) {
    hid = parseInt(hid);
    if (tc === null || tc === undefined) continue;
    console.log(hid, "tc:", tc);
    tc_bulk.push({
      updateOne: {
        filter: { hid },
        update: { $set: { hid, tc } },
        upsert: true,
      },
    });
  }
  if (!_.isEmpty(tc_bulk))
    await zed_db.db.collection("horse_details").bulkWrite(tc_bulk);
  await odds_generator_for_hids(hids);
  await breed_generator_for_hids(hids);
  await breed_generator_for_hids(parents_hids);
};

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const runner = async () => {
  await initiate();
  await init_btbtz();
  let ob = { 87728: 1 };
  await update_odds_and_breed_for_race_horses(ob);
};
// runner();

module.exports = {
  initiate,
  breed_generator_all_horses,
  odds_generator_all_horses,
  update_odds_and_breed_for_race_horses,
  odds_generator_for_hids,
  breed_generator_errs,
  odds_flames_generator_all_horses,
  breed_generator_m1_all_horses,
  blood_generator_all_horses,
  blood_generator_dist_all_horses,
  blood_generator_both_all_horses,
  general_bulk_push,
};
