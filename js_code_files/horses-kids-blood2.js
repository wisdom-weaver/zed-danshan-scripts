const _ = require("lodash");
const { init, zed_db, zed_ch } = require("./index-run");
const { calc_avg, pad, calc_median, fetch_r } = require("./utils");
const app_root = require("app-root-path");
const { download_eth_prices } = require("./base");

let mx = 11000;
let st = 1;
let ed = mx;
let chunk_size = 25;
let chunk_delay = 100;

//global
let z_ALL = {};
let tot_runs = 1;

const initiate = async () => {
  console.log("## Initiating");
  await init();
  await download_eth_prices();
  z_ALL = await get_z_ALL_meds();
};

const get_parents_hids = async (hid) => {
  hid = parseInt(hid);
  let { parents } = await zed_db.db
    .collection("horse_details")
    .findOne({ hid }, { projection: { _id: 0, parents: 1 } });
  return parents;
};

const get_z = (genotype) => {
  if (genotype.startsWith("Z")) genotype = genotype.slice(1);
  return parseInt(genotype);
};

const get_kids_existing = async (hid) => {
  try {
    hid = parseInt(hid);
    let { offsprings } = await zed_db.db
      .collection("horse_details")
      .findOne({ hid: 3312 }, { projection: { _id: 0, offsprings: 1 } });
    let ob = await Promise.all(
      offsprings.map((hid) =>
        zed_db.db.collection("horse_details").findOne(
          { hid },
          {
            projection: {
              _id: 0,
              hid: 1,
              genotype: 1,
              bloodline: 1,
              breed_type: 1,
              parents: 1,
            },
          }
        )
      )
    );
    ob = ob.map((e) => {
      let z = get_z(e.genotype);
      return { ...e, z, ...e.parents };
    });
    return ob;
  } catch (err) {
    console.log(err);
    return [];
  }
};

const get_breed_rating = async (hid) => {
  if (hid === null) return null;
  let { br = null } =
    (await zed_db.db
      .collection("rating_breed2")
      .findOne({ hid }, { projection: { _id: 0, br: 1 } })) || {};
  return br;
};

const get_g_odds = async (hid) => {
  hid = parseInt(hid);
  let coll = hid <= 8200 ? "odds_overall" : "odds_overall2";
  let ob = await zed_db.db
    .collection(coll)
    .findOne({ hid }, { projection: { "odds_overall.0#####": 1, _id: 0 } });
  let { odds_overall = {} } = ob || {};
  let g = odds_overall["0#####"] || null;
  return g;
};

const get_z_med_kid_score = async ({ bloodline, breed_type, genotype }) => {
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

const generate_breed_rating = async (hid) => {
  try {
    hid = parseInt(hid);
    if (hid == null || isNaN(hid)) return null;
    let kids = (await get_kids_existing(hid)) || [];
    // console.log({ hid, kids });
    if (_.isEmpty(kids)) {
      let empty_kg = {
        hid,
        odds: {},
        avg: null,
        br: null,
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
      kids.map((kid) => get_z_med_kid_score(kid).then((z) => [kid.hid, z]))
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
    console.table(kids);

    kids = kids.map((kid) => {
      let { g, z_med, other_parent_br } = kid;
      let kid_score =
        g === null
          ? null
          : _.mean([g - z_med, other_parent_br ? -other_parent_br : 0]);
      // let kid_score = g === null ? null : _.mean([g - z_med]);
      return { ...kid, kid_score };
    });

    let odds = _.chain(kids).keyBy("hid").mapValues("g").value();
    let kid_scores = _.chain(kids).keyBy("hid").mapValues("kid_score").value();

    let vals = _.chain(kid_scores).values().compact().value();
    let avg = calc_avg(vals) ?? null;
    let br = calc_median(vals) ?? null;

    let kg = { hid, odds, avg, br, kids_n, is };
    // console.log({ avg: dec2(avg), br: dec2(br) });
    // console.log({ hid, odds, kid_scores, avg, br });
    return kg;
  } catch (err) {
    console.log("err on horse get_kg", hid);
    console.log(err);
    return null;
  }
};

const get_kids_and_upload = async (hid, print = 0) => {
  try {
    hid = parseInt(hid);
    if (hid == null || isNaN(hid)) return null;
    let kg = await generate_breed_rating(hid);
    if (print == 1) {
      let { avg, br, odds, kids_n } = kg;
      let str = _.values(odds)
        .map((e) => (e == null ? "N" : parseFloat(e).toFixed(0)))
        .join(" ");
      console.log(`horse_${hid}`, ` (${kids_n})=>`, str);
      console.log({ avg, br });
    }
    if (_.isEmpty(kg)) {
      console.log("get_kids_and_upload", hid, "empty_kg");
      return;
    }
    // console.log(kg.is, hid, "=>", kg.kids_n, "kids, breed_rat:", kg.br);
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
    await initiate();
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

const runner = async () => {
  await initiate();
  let hid = 26885;
  let ob = await generate_breed_rating(hid);
  console.log(ob);
  console.log("ended");
};
// runner();

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

module.exports = {
  generate_breed_rating,
};
