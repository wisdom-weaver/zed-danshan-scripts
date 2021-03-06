const _ = require("lodash");
const fetch = require("node-fetch");
const mongoose = require("mongoose");
const { init, zed_db, zed_ch, run_func } = require("./index-run");
const {
  write_to_path,
  read_from_path,
  calc_avg,
  pad,
  calc_median,
  dec2,
  fetch_r,
} = require("./utils");
const app_root = require("app-root-path");
const {
  get_fee_cat_on,
  download_eth_prices,
  get_at_eth_price_on,
  get_date,
} = require("./base");
const { generate_max_horse } = require("./max_horses");
const { write_horse_details_to_hid_br } = require("./breed_score_leaderboard");

let st, ed, mx;
let chunk_size = 25;
let chunk_delay = 100;

//global
let z_ALL = {};
let tot_runs = 1;

const initiate_everything = async () => {
  console.log("## Initiating");
  await download_eth_prices();
  st = 0;
  mx = 82480;
  // mx = await generate_max_horse();
  ed = mx;
};

const get_parents_hids = async (hid) => {
  let api = `https://api.zed.run/api/v1/horses/get/${hid}`;
  let doc = await fetch_r(api);
  let parents = doc?.parents || { mother: null, father: null };
  parents = _.entries(parents).map(([role, doc]) => [
    role,
    doc?.horse_id || null,
  ]);
  parents = Object.fromEntries(parents);
  return parents;
};

const get_kids = async ({ hid, after = null }) => {
  try {
    let after_snip = after ? `?after=${after}` : "";
    let kids_api = `https://api.zed.run/api/v1/horses/offsprings/${hid}${after_snip}`;
    let kids = await fetch_r(kids_api).then((data) => {
      if (!data) return [];
      if (_.isEmpty(data.horses)) return [];
      let kids = data?.horses.map((ea) => ({
        hid: ea.horse_id,
        genotype: ea.genotype,
        bloodline: ea.bloodline,
        breed_type: ea.breed_type,
      }));
      if (data.page_info.has_next_page == true) after = data.page_info.after;
      else after = null;
      return kids;
    });
    let kids_parents = await Promise.all(
      _.map(kids || [], "hid").map((hid) =>
        get_parents_hids(hid).then((d) => [hid, d])
      )
    );
    kids_parents = Object.fromEntries(kids_parents);
    kids = kids.map((ea) => ({ ...ea, ...kids_parents[ea.hid] }));

    if (after !== null) kids = [...kids, ...(await get_kids({ hid, after }))];
    return kids;
  } catch (err) {
    console.log(err);
    console.log("err on get_kids", hid);
    return [];
  }
};

const get_details_of_hid_db = async (hid) => {
  hid = parseInt(hid);
  let doc = (await zed_db.db.collection("rating_blood").findOne({ hid })) || {};
  let dets = doc?.details || {};
  return { hid, ...dets };
};

const get_kids_existing = async ({ hid }) => {
  try {
    hid = parseInt(hid);
    let br_doc = await zed_db.db.collection("kids").findOne({ hid });
    let kids_hids = _.keys(br_doc?.odds || {});
    kids_hids = kids_hids.map((a) => parseInt(a));
    // console.log(kids_hids);
    let kids = await Promise.all(
      kids_hids.map((k) => get_details_of_hid_db(k))
    );
    kids = kids.map((k) => {
      let { parents, genotype, bloodline, breed_type } = k;
      return {
        hid: k.hid,
        genotype,
        bloodline,
        breed_type,
        ...parents,
      };
    });
    return kids;
    kids_parents = Object.fromEntries(kids_parents);
    kids_hids = kids_hids.map((ea) => ({ ...ea, ...kids_parents[ea.hid] }));
    return kids_hids;
  } catch (err) {
    console.log(err);
    return [];
  }
};

const get_breed_rating = async (hid) => {
  if (hid === null) return null;
  let doc = await zed_db.db.collection("kids").findOne({ hid });
  return doc?.gz_med || null;
};

const get_g_odds = async (hid) => {
  hid = parseInt(hid);
  let ob = await zed_db.db.collection("odds_overall").findOne({ hid });
  let { odds_overall = {} } = ob || {};
  let g = odds_overall["0#####"] || null;
  return g;
};

const get_z_med = async ({ bloodline, breed_type, genotype }) => {
  let id = "";
  let z = genotype.slice(1);
  z = "z" + pad(z, 3, 0).toString();
  bloodline = bloodline.toString().toLowerCase();
  breed_type = breed_type.toString().toLowerCase();
  id = `${z}-${bloodline}-${breed_type}`;
  if (_.isEmpty(z_ALL)) {
    let z_med_doc =
      (await zed_db.db.collection("z_meds").findOne({ id })) || {};
    let { med: z_med = 0 } = z_med_doc;
    return z_med;
  } else {
    let z_med = z_ALL[id];
    return z_med;
  }
};

const get_z_ALL_meds = async () => {
  let doc = await zed_db.db.collection("z_meds").findOne({ id: "z_ALL" });
  let ob = _.chain(doc.ar).keyBy("id").mapValues("med").value();
  return ob;
};

const get_kg = async (hid) => {
  try {
    hid = parseInt(hid);
    if (hid == null || isNaN(hid)) return null;
    let kids = (await get_kids_existing({ hid })) || [];
    // console.log({ hid, kids });
    if (_.isEmpty(kids)) {
      let empty_kg = {
        hid,
        odds: {},
        avg: null,
        gz_med: null,
        kids_n: 0,
        is: null,
      };
      return empty_kg;
    }

    let kids_n = _.isEmpty(kids) ? 0 : kids.length;
    let kids_hids = _.map(kids, "hid");

    let g_odds = await Promise.all(
      kids_hids.map((hid) => get_g_odds(hid).then((g) => [hid, g]))
    );
    g_odds = Object.fromEntries(g_odds);

    let z_meds = await Promise.all(
      kids.map((kid) => get_z_med(kid).then((z) => [kid.hid, z]))
    );
    z_meds = Object.fromEntries(z_meds);

    kids = kids.map((kid) => {
      let { mother, father } = kid;
      let other_parent = mother == hid ? father : mother;
      return { ...kid, other_parent };
    });
    let is = (kids && (kids[0].mother == hid ? "mom" : "dad")) || null;

    let other_parent_brs = await Promise.all(
      kids.map(({ hid, other_parent }) =>
        get_breed_rating(other_parent).then((m) => [hid, m])
      )
    );
    other_parent_brs = Object.fromEntries(other_parent_brs);

    kids = kids.map((kid) => ({
      ...kid,
      g: g_odds[kid.hid],
      z_med: z_meds[kid.hid],
      other_parent: kid.other_parent,
      other_parent_br: other_parent_brs[kid.hid],
    }));

    kids = kids.map((kid) => {
      let { g, z_med, other_parent_br } = kid;
      let kid_score =
        g === null
          ? null
          : _.mean([g - z_med, other_parent_br ? -other_parent_br : 0]);
      // let kid_score = g === null ? null : _.mean([g - z_med]);
      return { ...kid, kid_score };
    });
    // kids = kids.map((kid) => {
    //   const {
    //     hid,
    //     genotype,
    //     bloodline,
    //     breed_type,
    //     father,
    //     mother,
    //     other_parent,
    //     g,
    //     z_med,
    //     other_parent_br,
    //     kid_score,
    //   } = kid;
    //   return {
    //     hid,
    //     father,
    //     mother,
    //     other_parent,
    //     g,
    //     z_med,
    //     other_parent_br,
    //     kid_score,
    //   };
    // });
    // console.table(kids);

    console.table(kids);
    let odds = _.chain(kids).keyBy("hid").mapValues("g").value();
    let kid_scores = _.chain(kids).keyBy("hid").mapValues("kid_score").value();

    let vals = _.chain(kid_scores).values().compact().value();
    let avg = calc_avg(vals) ?? null;
    let gz_med = calc_median(vals) ?? null;
    let kg = { hid, odds, avg, gz_med, kids_n, is };
    // console.log({ avg: dec2(avg), gz_med: dec2(gz_med) });
    // console.log({ hid, odds, kid_scores, avg, gz_med });
    return kg;
  } catch (err) {
    // console.log(err);
    console.log("err on horse get_kg", hid);
    return null;
  }
};

const get_kids_and_upload = async (hid, print = 0) => {
  try {
    hid = parseInt(hid);
    if (hid == null || isNaN(hid)) return null;
    let kg = await get_kg(hid);
    if (print == 1) {
      let { avg, gz_med, odds, kids_n } = kg;
      let str = _.values(odds)
        .map((e) => (e == null ? "N" : parseFloat(e).toFixed(0)))
        .join(" ");
      console.log(`horse_${hid}`, ` (${kids_n})=>`, str);
      console.log({ avg, gz_med });
    }
    if (_.isEmpty(kg)) {
      console.log("get_kids_and_upload", hid, "empty_kg");
      return;
    }
    // console.log(kg.is, hid, "=>", kg.kids_n, "kids, breed_rat:", kg.gz_med);
    kg.db_date = new Date().toISOString();
    kg.fn = "script";
    await zed_db.db
      .collection("kids")
      .updateOne({ hid }, { $set: kg }, { upsert: true });
    // await write_horse_details_to_hid_br(hid);
  } catch (err) {
    console.log("ERR on get_kids_and_upload", hid);
  }
};

const get_all_horses_kids = async () => {
  try {
    await initiate_everything();
    // await download_eth_prices();
    // st = 530;
    // ed = 530;
    z_ALL = await get_z_ALL_meds();
    console.log("z_ALL loaded");

    console.log("=> STARTED horses_kids: ", `${st}:${ed}`);
    let hids = new Array(ed - st + 1).fill(0).map((ea, idx) => st + idx);

    for (let run = 1; run <= tot_runs; run++) {
      let i = 0;
      for (let chunk of _.chunk(hids, chunk_size)) {
        await Promise.all(chunk.map((hid) => get_kids_and_upload(hid, 1)));
        await delay(chunk_delay);
        console.log(`#RUN${run}`, chunk[0], " -> ", chunk[chunk.length - 1]);
        i++;
        if (i % 10 == 0) {
          console.log("------");
          await delay(1000);
          i = 0;
        }
      }
    }
    console.log("## Fetch completed");
  } catch (err) {
    console.log("ERROR get_all_horses_kids\n", err);
  }
};
// get_all_horses_kids();

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

module.exports = {
  get_kg,
  get_all_horses_kids,
  get_kids_and_upload,
  get_parents_hids,
};
