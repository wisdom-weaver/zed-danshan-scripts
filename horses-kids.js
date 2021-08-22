const _ = require("lodash");
const prompt = require("prompt-sync")();
const fetch = require("node-fetch");
const mongoose = require("mongoose");
const { init, zed_db, zed_ch, run_func } = require("./index-run");
const {
  write_to_path,
  read_from_path,
  calc_avg,
  pad,
  calc_median,
} = require("./utils");
const app_root = require("app-root-path");
const {
  get_fee_cat_on,
  download_eth_prices,
  get_at_eth_price_on,
  get_date,
} = require("./base");

let mx = 82000;
// let h = 3312;
let st = 0;
let ed = mx;
// st = h;
// ed = h;
let chunk_size = 25;
let chunk_delay = 100;

const get_kids = async ({ hid, after = null }) => {
  try {
    let after_snip = after ? `?after=${after}` : "";
    let kids_api = `https://api.zed.run/api/v1/horses/offsprings/${hid}${after_snip}`;
    let kids = await fetch(kids_api)
      .then((r) => r.json())
      .then((data) => {
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
    if (after !== null) kids = [...kids, ...(await get_kids({ hid, after }))];
    return kids;
  } catch (err) {
    console.log("err on get_kids", hid);
    return [];
  }
};

const get_g_odds = async (hid) => {
  hid = parseInt(hid);
  let ob = await zed_db.db.collection("odds_overall").findOne({ hid });
  let { odds_overall = {} } = ob || {};
  let g = odds_overall["0#####"] || null;
  return g;
};

const get_z_median_from_details = async ({
  bloodline,
  breed_type,
  genotype,
}) => {
  let id = "";
  let z = genotype.slice(1);
  z = "z" + pad(z, 3, 0).toString();
  bloodline = bloodline.toString().toLowerCase();
  breed_type = breed_type.toString().toLowerCase();
  id = `${z}-${bloodline}-${breed_type}`;
  let z_med_doc = (await zed_db.db.collection("z_meds").findOne({ id })) || {};
  let { med: z_med = 0 } = z_med_doc;
  // console.log(id, z_med);
  return z_med;
};

const get_kg = async (hid, i = 3) => {
  try {
    hid = parseInt(hid);
    if (i == 0) return null;
    if (hid == null || isNaN(hid)) return null;
    let kids = (await get_kids({ hid })) || [];
    let kids_n = _.isEmpty(kids) ? 0 : kids.length;
    let kids_hids = _.map(kids, "hid");
    let odds = await Promise.all(
      kids_hids.map((hid) => get_g_odds(hid).then((g) => [hid, g]))
    );
    odds = Object.fromEntries(odds);
    let avg = calc_avg(_.values(odds)) || null;

    let gz_med = await Promise.all(
      kids.map(async ({ hid, genotype, bloodline, breed_type }) => {
        let z_med = await get_z_median_from_details({
          genotype,
          bloodline,
          breed_type,
        });
        // console.log({ hid, z_med });
        return [hid, z_med];
      })
    );
    gz_med = gz_med?.map(([hid, z_med], i) => {
      if (odds[hid] == null) return null;
      return odds[hid] - z_med;
    });
    gz_med = _.compact(gz_med);
    gz_med = calc_median(gz_med) || null;
    return { hid, odds, avg, gz_med, kids_n };
  } catch (err) {
    console.log(err);
    console.log("err on horse get_kg", hid, "refetching....");
    await delay(100);
    return await get_kg(hid, i - 1);
    // return  null;
  }
};

const get_kids_and_upload = async (hid) => {
  hid = parseInt(hid);
  if (hid == null || isNaN(hid)) return null;
  let kg = await get_kg(hid);
  if(_.isEmpty(kg)){
    console.log("get_kids_and_upload", hid, "empty_kg");
    return;
  }
  console.log(hid, "=>", kg.kids_n, "kids, breed_rat:", kg.gz_med);
  await zed_db.db
    .collection("kids")
    .updateOne({ hid }, { $set: kg }, { upsert: true });
};

const get_all_horses_kids = async () => {
  try {
    await init();
    let hids = new Array(ed - st + 1).fill(0).map((ea, idx) => st + idx);
    console.log("=> STARTED horses_kids: ", `${st}:${ed}`);

    let i = 0;
    for (let chunk of _.chunk(hids, chunk_size)) {
      i += chunk_size;
      // console.log("\n=> fetching together:", chunk.toString());
      await Promise.all(chunk.map((hid) => get_kids_and_upload(hid)));
      await delay(chunk_delay);
      console.log("! got", chunk[0], " -> ", chunk[chunk.length - 1]);
      // if (i % 10000 == 0) generate_blood_mapping();
    }
    console.log("## Fetch completed");
  } catch (err) {
    console.log("ERROR get_all_horses_kids\n", err);
  }
};

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

module.exports = {
  get_all_horses_kids,
};
