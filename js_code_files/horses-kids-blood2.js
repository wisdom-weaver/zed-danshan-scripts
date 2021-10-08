const _ = require("lodash");
const { init, zed_db, zed_ch } = require("./index-run");
const {
  calc_avg,
  pad,
  calc_median,
  fetch_r,
  struct_race_row_data,
  dec,
} = require("./utils");
const app_root = require("app-root-path");
const { download_eth_prices, get_at_eth_price_on } = require("./base");
const { options } = require("./options");

let mx = 11000;
let st = 1;
let ed = mx;
let chunk_size = 25;
let chunk_delay = 100;

//global
let z_ALL = {};
let blbtz = {};
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
    let { offsprings = [] } =
      (await zed_db.db
        .collection("horse_details")
        .findOne({ hid }, { projection: { _id: 0, offsprings: 1 } })) || {};
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
      if (_.isEmpty(e)) return null;
      let z = get_z(e.genotype);
      return { ...e, z, ...e.parents };
    });
    ob = _.compact(ob);
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
  // let coll = hid <= 82000 ? "odds_overall" : "odds_overall2";
  let coll = "odds_overall2";
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

const get_blbtz_global_avg = async ({ bloodline, breed_type, genotype }) => {
  let id = `${bloodline}-${breed_type}-${genotype}`;
  return blbtz[id]?.avg || null;
};

const generate_breed_rating_old = async (hid) => {
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
      console.log("# hid:", hid, 0, "br:", null);
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

    kids = kids.map((kid) => {
      let { g, z_med, other_parent_br } = kid;
      let kid_score =
        g === null
          ? null
          : _.mean([g - z_med, other_parent_br ? -other_parent_br : 0]);
      // let kid_score = g === null ? null : _.mean([g - z_med]);
      return { ...kid, kid_score };
    });

    // console.table(kids);

    let odds = _.chain(kids).keyBy("hid").mapValues("g").value();
    let kid_scores = _.chain(kids).keyBy("hid").mapValues("kid_score").value();

    let vals = _.chain(kid_scores).values().compact().value();
    let avg = calc_avg(vals) ?? null;
    let br = calc_median(vals) ?? null;

    let kg = { hid, odds, avg, br, kids_n, is };
    // console.log({ avg: dec2(avg), br: dec2(br) });
    console.log("# hid:", hid, kids_n, "br:", br);
    return kg;
  } catch (err) {
    console.log("err on horse get_kg", hid);
    // console.log(err);
    return null;
  }
};

const get_kids_score = async (hid, p = 0) => {
  try {
    hid = parseInt(hid);
    let races = await zed_ch.db
      .collection("zed")
      .find({ 6: hid })
      .sort({ 2: 1 })
      .limit(5)
      .toArray();
    if (_.isEmpty(races)) return null;
    // console.log(hid);
    races = struct_race_row_data(races);
    races = races.map((i) => {
      let eth_price = get_at_eth_price_on(i.date);
      let entryfee_usd = parseFloat(i.entryfee) * eth_price;
      return { ...i, entryfee_usd, eth_price };
    });
    if (p) console.table(races);

    let races_n = races.length;
    let flames_per = _.filter(races, { flame: 1 });
    flames_per = (1.5 * ((flames_per?.length || 0) * 100)) / races_n;

    let entryfee_2xsum = _.chain(races).map("entryfee_usd").sum().value();
    entryfee_2xsum = entryfee_2xsum * 2;
    entryfee_2xsum = Math.min(entryfee_2xsum, 50);

    if (p)
      console.table(
        races.map((e) => {
          let { entryfee, entryfee_usd, eth_price, flame, place } = e;
          return { entryfee, entryfee_usd, eth_price, flame, place };
        })
      );

    let p1_count =
      _.filter(races, (i) => i.place?.toString() === "1")?.length || 0;
    let p1_calc = (1 * (p1_count * 100)) / races_n;
    let p2_count =
      _.filter(races, (i) => i.place?.toString() === "2")?.length || 0;
    let p2_calc = (0.65 * (p2_count * 100)) / races_n;
    let p11_count =
      _.filter(races, (i) => i.place?.toString() === "11")?.length || 0;
    let p11_calc = (0.3 * (p11_count * 100)) / races_n;
    let p12_count =
      _.filter(races, (i) => i.place?.toString() === "12")?.length || 0;
    let p12_calc = (0.4 * (p12_count * 100)) / races_n;

    let races_Nx8 = races_n * 8;
    let kid_score = 0;
    if (p)
      console.table([
        {
          races_n,
          races_Nx8,
          flames_per,
          entryfee_2xsum,
          p1_count,
          p1_calc,
          p2_count,
          p2_calc,
          p11_count,
          p11_calc,
          p12_count,
          p12_calc,
        },
      ]);
    kid_score =
      (races_Nx8 +
        flames_per +
        entryfee_2xsum +
        p1_calc +
        p2_calc +
        p11_calc +
        p12_calc) /
      100;
    if (p) console.log(hid, kid_score);
    return kid_score;
  } catch (err) {
    console.log("err in get_kids_score", err);
  }
};

const generate_breed_rating = async (hid, p = 0) => {
  try {
    let hid_kid_score = await get_kids_score(hid);
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
        kid_score: hid_kid_score,
      };
      console.log(
        "# hid:",
        hid,
        "kids_n:",
        0,
        "br:",
        null,
        "hid_kid_score:",
        dec(hid_kid_score, 2)
      );
      return empty_kg;
    }

    let kids_n = _.isEmpty(kids) ? 0 : kids.length;
    let kids_hids = _.map(kids, "hid");

    let kids_scores_ob = await Promise.all(
      kids.map((kid) => get_kids_score(kid.hid).then((z) => [kid.hid, z]))
    );
    kids_scores_ob = Object.fromEntries(kids_scores_ob);

    let gavg_ob = await Promise.all(
      kids.map((kid) =>
        get_blbtz_global_avg(kid).then((gavg) => [kid.hid, gavg])
      )
    );
    gavg_ob = Object.fromEntries(gavg_ob);

    let op_br_ob = await Promise.all(
      kids.map((kid) => {
        let { father, mother } = kid.parents;
        let op = hid == father ? mother : father;
        return get_breed_rating(op).then((op_br) => [kid.hid, op_br]);
      })
    );
    op_br_ob = Object.fromEntries(op_br_ob);

    kids = kids.map((e) => ({
      ...e,
      kid_score: kids_scores_ob[e.hid],
      gavg: gavg_ob[e.hid],
      op_br: op_br_ob[e.hid],
    }));
    kids = kids.map((e) => {
      let fact;
      if (
        e.kid_score == 0 ||
        e.gavg == 0 ||
        _.isNaN(e.kid_score) ||
        _.isNaN(e.gavg)
      )
        fact = null;
      else fact = e.kid_score / e.gavg;
      let adj;
      if (fact == null) adj = null;

      if (e.op_br == null || _.isNaN(e.op_br)) {
        adj = fact;
      } else {
        adj = e.op_br > 1.1 ? fact * 0.9 : e.op_br < 0.9 ? fact * 1.1 : fact;
      }
      let good_adj;
      if (adj == 0) good_adj = 0;
      else good_adj = e.kid_score > e.gavg ? 0.1 : -0.1;
      return { ...e, fact, adj, good_adj };
    });
    if (p) console.table(kids);

    let avg = _.chain(kids_scores_ob).values().compact().mean().value();
    let br = _.chain(kids).map("adj").values().compact().value();
    if (br.length == 0) br = null;
    else br = _.mean(br);

    br += _.chain(kids).map("good_adj").compact().sum().value();

    let kg = {
      hid,
      odds: kids_scores_ob,
      avg,
      br,
      kids_n,
      kid_score: hid_kid_score,
    };
    // console.log({ avg: dec2(avg), br: dec2(br) });
    console.log(
      "# hid:",
      hid,
      "kids_n:",
      kids_n,
      "br:",
      dec(br, 2),
      "hid_kid_score:",
      dec(hid_kid_score, 2)
    );
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

// const get_all_horses_kids = async () => {
//   try {
//     await initiate();
//     z_ALL = await get_z_ALL_meds();
//     console.log("z_ALL loaded");

//     console.log("=> STARTED horses_kids: ", `${st}:${ed}`);
//     let hids = new Array(ed - st + 1).fill(0).map((ea, idx) => st + idx);

//     for (let run = 1; run <= tot_runs; run++) {
//       let i = 0;
//       for (let chunk of _.chunk(hids, chunk_size)) {
//         await Promise.all(chunk.map((hid) => get_kids_and_upload(hid, 1)));
//         await delay(chunk_delay);
//         console.log(`#RUN${run}`, chunk[0], " -> ", chunk[chunk.length - 1]);
//         i++;
//         if (i % 10 == 0) {
//           console.log("------");
//           await delay(1000);
//           i = 0;
//         }
//       }
//     }
//     console.log("## Fetch completed");
//   } catch (err) {
//     console.log("ERROR get_all_horses_kids\n", err);
//   }
// };

const push_kids_score_bulk = async ({ ar, chunk_hids }) => {
  let bulk = [];
  for (let ea of ar) {
    if (_.isEmpty(ea)) continue;
    let { hid, kid_score } = ea;
    // console.log({ hid, kid_score });
    bulk.push({
      updateOne: {
        filter: { hid },
        update: { $set: { kid_score: kid_score } },
        upsert: true,
      },
    });
  }
  await zed_db.db.collection("rating_breed2").bulkWrite(bulk);
  console.log("wrote bulk", bulk.length, chunk_hids[chunk_hids.length - 1]);
};

const push_kids_score_all_horses = async () => {
  await initiate();
  let st = 1;
  let ed = 135000;
  let cs = 500;
  let hids = new Array(ed - st + 1).fill(0).map((e, i) => i + st);
  // let hids = [1102];
  // let miss = await zed_db.db
  //   .collection("rating_breed2")
  //   .find({ kid_score: { $exists: false } }, { projection: { hid: 1 } })
  //   .toArray();
  // let hids = _.map(miss, "hid");
  // console.log("missing.len", hids.length);
  for (let chunk_hids of _.chunk(hids, cs)) {
    let ar = await Promise.all(
      chunk_hids.map((hid) =>
        get_kids_score(hid).then((kid_score) => ({ hid, kid_score }))
      )
    );
    // console.log(ar);
    await push_kids_score_bulk({ ar, chunk_hids });
  }
  console.log("ended");
};
// push_kids_score_all_horses();
const geno = (z) => {
  if (z.startsWith("Z")) z = z.slice(1);
  return parseFloat(z);
};
const get_z_table_for_id = async (id) => {
  let [bl, bt, z] = id.split("-");
  let ar = await zed_db.db
    .collection("horse_details")
    .find(
      {
        bloodline: bl,
        breed_type: bt,
        genotype: z,
      },
      { projection: { _id: 0, hid: 1 } }
    )
    .toArray();
  let hids = _.map(ar, "hid") || [];
  let docs = await zed_db.db
    .collection("rating_breed2")
    .find(
      { hid: { $in: hids } },
      { projection: { _id: 0, kid_score: 1, br: 1 } }
    )
    .toArray();

  let scores = _.chain(docs).map("kid_score").compact().value();
  let brs = _.chain(docs).map("br").compact().value();

  let avg = _.mean(scores);
  if (!avg || _.isNaN(avg)) avg = null;
  let ks_min = _.min(scores);
  if (!ks_min || _.isNaN(ks_min)) ks_min = null;
  let ks_max = _.max(scores);
  if (!ks_max || _.isNaN(ks_max)) ks_max = null;

  let br_avg = _.mean(brs);
  if (!br_avg || _.isNaN(br_avg)) br_avg = null;
  let br_min = _.min(brs);
  if (!br_min || _.isNaN(br_min)) br_min = null;
  let br_max = _.max(brs);
  if (!br_max || _.isNaN(br_max)) br_max = null;

  let str = [ks_min, avg, ks_max, br_min, br_avg, br_max]
    .map((e) => dec(e))
    .join(" ");

  console.log({ id, count: scores.length }, str);
  return {
    count_all: ar.length,
    count: scores.length,
    count_: brs.length,
    ks_min,
    avg,
    ks_max,
    br_min,
    br_avg,
    br_max,
  };
};
const blood_breed_z_table = async () => {
  await init();
  let ob = {};
  let keys = [];
  for (let bl of options.bloodline)
    for (let bt of options.breed_type)
      for (let z of options.genotype) {
        if (bt == "genesis" && geno(z) > 10) continue;
        if (bl == "Nakamoto" && bt == "legendary" && geno(z) > 4) continue;
        if (bl == "Szabo" && bt == "legendary" && geno(z) > 8) continue;
        if (bl == "Finney" && bt == "legendary" && geno(z) > 14) continue;
        if (bl == "Buterin" && bt == "legendary" && geno(z) > 20) continue;
        let id = `${bl}-${bt}-${z}`;
        keys.push(id);
      }

  // keys = keys.slice(0, 5);
  for (let id of keys) {
    ob[id] = await get_z_table_for_id(id);
  }

  let doc_id = "kid-score-global";
  await zed_db.db
    .collection("requirements")
    .updateOne(
      { id: doc_id },
      { $set: { id: doc_id, avg_ob: ob } },
      { upsert: true }
    );
  console.log("done");
};
// blood_breed_z_table();

const init_btbtz = async () => {
  let doc_id = "kid-score-global";
  let doc = await zed_db.db.collection("requirements").findOne({ id: doc_id });
  blbtz = doc.avg_ob;
  console.log("#done init_btbtz");
};

const runner = async () => {
  await init();
  let doc_id = "kid-score-global";
  let doc = await zed_db.db.collection("requirements").findOne({ id: doc_id });
  let ob = doc.avg_ob;
  ob = _.entries(ob).map((i) => {
    return { id: i[0], ...i[1] };
  });
  console.table(ob);
};
// runner();

const runner2 = async () => {
  await init();
  await init_btbtz();
  let hids = [91288];
  // let hids = [24865, 22558, 20501, 24538, 26646, 24865, 22558, 20501, 24538];
  for (let hid of hids) {
    let br = await generate_breed_rating(hid, 1);
    console.log(br);
    console.log("done");
  }
};
// runner2();
const runner3 = async () => {
  await init();
  await download_eth_prices();
  await init_btbtz();
  let hid = 1109;
  let ks = await generate_breed_rating(hid, 1);
  console.log(ks);
  // await zed_db.db.collection("rating_breed2").updateOne({ hid }, { $set: { br: 1 } });
  console.log("done");
};
runner3();

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

module.exports = {
  push_kids_score_all_horses,
  generate_breed_rating,
  push_kids_score_all_horses,
  blood_breed_z_table,
  init_btbtz,
};
