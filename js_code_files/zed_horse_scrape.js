const { init, zed_db } = require("./index-run");
const _ = require("lodash");
const {
  driver_test,
  get_webdriver,
  elems_by_x,
  elem_by_x,
  elem_by_x_txt,
} = require("./webdriver-utils");
const kyh_url = (hid) => `https://knowyourhorses.com/horses/${hid}`;
const hawku_url = (hid) => `https://www.hawku.com/horse/${hid}`;
const { delay, write_to_path, read_from_path } = require("./utils");
const appRootPath = require("app-root-path");
const { fetch_a } = require("./fetch_axios");
const {
  odds_generator_for_hids,
  update_odds_and_breed_for_race_horses,
} = require("./odds-generator-for-blood2");
const zedf = require("./zedf");
const cron_parser = require("cron-parser");
const cron = require("node-cron");
const { fix_horse_type_using_kid_ids } = require("./parents-blood2");

// X-paths
// name            /html/body/ul/div/main/main/div[1]/div[1]/div[2]/h1
// dets            /html/body/ul/div/main/main/div[1]/div[1]/div[2]/p[1]/span
// color           /html/body/ul/div/main/main/div[1]/div[1]/div[2]/p[2]
// owner slug      /html/body/ul/div/main/main/div[1]/div[1]/div[2]/p[3]/a
// birthday        /html/body/ul/div/main/main/div[1]/div[1]/div[2]/p[4]
// img             /html/body/ul/div/main/main/div[1]/div[1]/div[1]/div/img
// familybtn       /html/body/ul/div/main/main/a
// offspringbtn    /html/body/ul/div/main/main/div[4]/h3
//                 /html/body/ul/div/main/main/div[4]/h3
//                 /html/body/ul/div/main/main/div[5]/h3
// fam_part_cont   /html/body/ul/div/main/main/div[4]
//                 /html/body/ul/div/main/main/div[4]/h3
// Part/kids row   /html/body/ul/div/main/main/div[4]/div
// Part/kids elem  /html/body/ul/div/main/main/div[4]/div/div[1]
// elem atag hid   /html/body/ul/div/main/main/div[4]/div/div[1]/div[2]/a

let base_x = "/html/body/ul/div/main/main";
let name_xt = "div[1]/div[1]/div[2]/h1";
let dets_xt = "div[1]/div[1]/div[2]/p[1]/span";
let colr_xt = "div[1]/div[1]/div[2]/p[2]";
let owner_xt = "div[1]/div[1]/div[2]/p[3]/a";
let bday_xt = "div[1]/div[1]/div[2]/p[4]";
let img_xt = "div[1]/div[1]/div[1]/div/img";
let fam_btn = "a";
let fam_p1_cont = "div[4]";
let fam_p2_cont = "div[5]";
let fam_p_txt = "h3";
let fam_p_elems = "div/div";
let fam_p_hid = "div[2]/a";

const get_class = (rating) => {
  rating = parseInt(rating);
  let ar = [
    [0, 20, 5],
    [21, 40, 4],
    [41, 60, 3],
    [61, 80, 2],
    [80, 1e14, 1],
  ];
  for (let [mi, mx, c] of ar) if (_.inRange(rating, mi, mx + 0.1)) return c;

  return null;
};
const extract_name = async (cont) => {
  try {
    let name = await elem_by_x_txt(cont, name_xt);
    return name || "";
  } catch (err) {
    return "";
  }
};
const extract_dets = async (cont) => {
  try {
    let dets = await elem_by_x_txt(cont, dets_xt);
    if (!dets) return {};
    let [genotype, bloodline, breed_type, horse_type, rating] = dets.split(" ");
    rating = rating.slice(1);
    rating = parseInt(rating);
    let tc = get_class(rating);
    let ob = {
      genotype,
      bloodline,
      breed_type,
      horse_type,
      rating,
      tc,
    };
    let color = await elem_by_x_txt(cont, colr_xt);
    ob.color = color;
    return ob || {};
  } catch (err) {
    // console.log(err);
    return {};
  }
};
const extract_owner = async (cont) => {
  try {
    let dets = await elem_by_x(cont, owner_xt);
    if (!dets) return {};
    let slug = await dets.getText();
    let owner_id = await dets.getAttribute("href");
    owner_id = owner_id.slice(owner_id.lastIndexOf("/") + 1);
    let ob = {
      slug,
      owner_id,
    };
    return ob || {};
  } catch (err) {
    // console.log(err);
    return {};
  }
};
const click_fam_btn = async (cont) => {
  try {
    let elem = await elem_by_x(cont, fam_btn);
    let txt = await elem.getText();
    await elem.click();
    await delay(2000);
  } catch (err) {
    // console.log(err);
  }
};
const extract_hex_code = async (cont) => {
  try {
    let img_el = await elem_by_x(cont, img_xt);
    // console.log(img_el);
    let hex_code = await img_el.getAttribute("src");
    // console.log(hex_code);
    hex_code = hex_code.slice(hex_code.lastIndexOf("/") + 1);
    hex_code = hex_code.replaceAll(".svg", "");
    // console.log(hex_code)
    return { hex_code };
  } catch (err) {
    // console.log(err);
    return { hex_code: null };
  }
};
const extract_p_hid = async (p_elem) => {
  try {
    let a_tag = await elem_by_x(p_elem, fam_p_hid);
    let hid = await a_tag.getAttribute("href");
    hid = hid.slice(hid.lastIndexOf("/") + 1);
    hid = parseInt(hid);
    return hid || null;
  } catch (err) {
    // console.log(err);
    return null;
  }
};
const def_fam_ob = {
  parents: { mother: null, father: null },
  offsprings: [],
};
const get_fam_part = async (cont) => {
  let ob = def_fam_ob;
  try {
    try {
      let p1 = await elem_by_x(cont, fam_p1_cont);
      let p1_txt = await elem_by_x_txt(p1, fam_p_txt);
      // console.log(p1_txt);
      let p1_elems = await elems_by_x(p1, fam_p_elems);
      let hids = [];
      for (let el of p1_elems) {
        hids.push(await extract_p_hid(el));
      }
      // console.log(hids);
      if (p1_txt == "Parents")
        ob.parents = {
          mother: hids[0],
          father: hids[1],
        };
      else if (p1_txt == "Offspring") ob.offsprings = hids;
    } catch (err) {
      // console.log(err);
    }
    try {
      let p2 = await elem_by_x(cont, fam_p2_cont);
      let p2_txt = await elem_by_x_txt(p2, fam_p_txt);
      // console.log(p2_txt);
      let p2_elems = await elems_by_x(p2, fam_p_elems);
      let hids = [];
      for (let el of p2_elems) {
        hids.push(await extract_p_hid(el));
      }
      // console.log(hids);
      if (p2_txt == "Offspring") ob.offsprings = hids;
    } catch (err) {
      // console.log(err);
    }
    return ob;
  } catch (err) {
    // console.log(err);
    return def_fam_ob;
  }
};
const scrape_horse_details = async (hid) => {
  let driver;
  try {
    hid = parseInt(hid);
    let url = hawku_url(hid);
    // console.log(url);
    driver = await get_webdriver();
    if (driver == null) return null;
    await driver.get(url);
    await driver.sleep(3000);
    let ob = { hid };
    let page = await elem_by_x(driver, base_x);
    ob.name = await extract_name(page);
    let dets = await extract_dets(page);
    let own = await extract_owner(page);
    let hx = await extract_hex_code(page);
    ob = { ...ob, ...dets, ...own, ...hx };
    await click_fam_btn(page);
    let fam = await get_fam_part(page);
    ob = { ...ob, ...fam };
    if (driver && driver.quit) await driver.quit();
    let parents = ob?.parents;
    // console.log({ hid, parents });
    await zed_db
      .collection("horse_details")
      .updateOne({ hid }, { $set: ob }, { upsert: true });
    await upload_kid_id_to_parents({ hid, parents });
    ob.parents_d = await get_parents_d_for_kid({ hid, parents });
    return ob;
  } catch (err) {
    console.log("err on scrape_horse_details", hid);
    // console.log("err on scrape_horse_details", err);
    if (driver && driver.quit) await driver.quit();
    return null;
  }
};

const get_n_upload_horse_details = async (hid) => {
  try {
    let data = await scrape_horse_details(hid);
    console.log(hid, data);
    await zed_db.db
      .collection("horse_details")
      .updateOne({ hid }, { $set: data }, { upsert: true });
  } catch (err) {
    console.log("mongo err", hid);
  }
};

const zed_horses_all_scrape = async () => {
  await init();
  console.log("started");
  let st = 95566;
  let ed = 111000;
  let hids = new Array(ed - st + 1).fill(0).map((e, i) => i + st);
  let cs = 5;
  let i = 0;
  for (let chunk of _.chunk(hids, cs)) {
    await Promise.all(chunk.map((hid) => get_n_upload_horse_details(hid)));
    await delay(2000);
    if (i % 10 == 0) await delay(10000);
    console.log("done", chunk.toString());
    i++;
  }
  console.log("completed");
};

const upload_kid_id_to_parents = async ({ hid, parents = {} }) => {
  try {
    let { mother = null, father = null } = parents || {};
    if (mother) {
      mother = parseInt(mother);
      await zed_db
        .collection("horse_details")
        .updateOne(
          { hid: mother },
          { $set: { hid: mother }, $addToSet: { offsprings: hid } },
          { upsert: true }
        );
    }
    if (father) {
      father = parseInt(father);
      await zed_db
        .collection("horse_details")
        .updateOne(
          { hid: father },
          { $set: { hid: father }, $addToSet: { offsprings: hid } },
          { upsert: true }
        );
    }
  } catch (err) {
    console.log("err upload_kid_id_to_parents", err.message);
  }
};

const get_parents_d_for_kid = async ({ hid, parents = {} }) => {
  try {
    let { mother, father } = parents || {};
    let parents_d = {};
    if (mother) {
      mother = parseInt(mother);
      let doc = await zed_db.collection("horse_details").findOne(
        { hid: mother },
        {
          projection: {
            _id: 0,
            bloodline: 1,
            breed_type: 1,
            horse_type: 1,
            genotype: 1,
          },
        }
      );
      parents_d.mother = doc;
    } else {
      parents_d.mother = null;
    }
    if (father) {
      father = parseInt(father);
      let doc = await zed_db.collection("horse_details").findOne(
        { hid: father },
        {
          projection: {
            _id: 0,
            bloodline: 1,
            breed_type: 1,
            horse_type: 1,
            genotype: 1,
          },
        }
      );
      parents_d.father = doc;
    } else {
      parents_d.father = null;
    }
    return parents_d;
  } catch (err) {
    console.log(err);
    return { mother: null, father: null };
  }
};

const upload_parents_d_for_kid = async (hid) => {
  // let doc = zed_db.db.collection.find
  // get_parents_d_for_kid
};

const add_horse_dets_old = async (hid) => {
  hid = parseInt(hid);
  let blood_doc = await zed_db.db.collection("rating_blood").findOne({ hid });
  let { rating_blood, name, details, rank } = blood_doc;
  let {
    bloodline,
    thisclass: tc,
    breed_type,
    genotype,
    color,
    gender,
    horse_type,
    hex_code,
    owner_stable_slug: slug,
    parents,
  } = details;
  let ob = {
    hid,
    name,
    bloodline,
    tc,
    breed_type,
    genotype,
    color,
    gender,
    horse_type,
    hex_code,
    slug,
    parents,
  };

  await upload_kid_id_to_parents({ hid, parents });
  console.log({ hid, mother, father });
};

const get_horses_dets_old = async (hids) => {
  let blood_docs = await zed_db.db
    .collection("rating_blood")
    .find(
      { hid: { $in: hids } },
      {
        projection: {
          _id: 0,
          hid: 1,
          name: 1,
          "details.bloodline": 1,
          "details.thisclass": 1,
          "details.breed_type": 1,
          "details.genotype": 1,
          "details.color": 1,
          "details.gender": 1,
          "details.horse_type": 1,
          "details.hex_code": 1,
          "details.owner_stable_slug": 1,
          "details.parents": 1,
          "details.parents_d.mother.bloodline": 1,
          "details.parents_d.mother.breed_type": 1,
          "details.parents_d.mother.genotype": 1,
          "details.parents_d.mother.horse_type": 1,
          "details.parents_d.father.bloodline": 1,
          "details.parents_d.father.breed_type": 1,
          "details.parents_d.father.genotype": 1,
          "details.parents_d.father.horse_type": 1,
        },
      }
    )
    .toArray();
  blood_docs = blood_docs.map((blood_doc) => {
    let { hid, name, details } = blood_doc;
    let {
      bloodline,
      thisclass: tc,
      breed_type,
      genotype,
      color,
      gender,
      horse_type,
      hex_code,
      owner_stable_slug: slug,
      parents = {},
      parents_d = {},
    } = details;
    if (parents?.mother == null) parents_d.mother = null;
    if (parents?.father == null) parents_d.father = null;

    let ob = {
      hid,
      name,
      bloodline,
      tc,
      breed_type,
      genotype,
      color,
      gender,
      horse_type,
      hex_code,
      slug,
      parents,
      parents_d,
    };
    return ob;
  });
  return blood_docs;
};

const add_horse_rating_blood = async (hid) => {
  hid = parseInt(hid);
  let blood_doc = await zed_db.db.collection("rating_blood").findOne({ hid });
  let { rating_blood = {}, name } = blood_doc;
  let ob = {
    hid,
    name,
    ...rating_blood,
  };
  await zed_db.db
    .collection("rating_blood2")
    .updateOne({ hid }, { $set: ob }, { upsert: true });
};

const add_horse_rating_flames = async (hid) => {
  hid = parseInt(hid);
  let doc = await zed_db.db.collection("rating_flames").findOne({ hid });
  await zed_db.db
    .collection("rating_flames2")
    .updateOne({ hid }, { $set: doc }, { upsert: true });
};
const add_horse_rating_breed = async (hid) => {
  hid = parseInt(hid);
  let doc = await zed_db.db.collection("kids").findOne({ hid });
  let { avg, gz_med: br, kids_n, odds } = doc;
  let ob = {
    avg,
    br,
    kids_n,
    odds,
  };
  await zed_db.db
    .collection("rating_breed2")
    .updateOne({ hid }, { $set: ob }, { upsert: true });
};

const add_horse_ratings = async (hid) => {
  try {
    await Promise.all([
      add_horse_rating_blood(hid),
      add_horse_rating_flames(hid),
      add_horse_rating_breed(hid),
    ]);
    console.log(hid);
  } catch (err) {
    console.log("err on ", hid);
  }
};

const zed_add_horses_from_old_collection = async () => {
  await init();
  let st = 17649;
  let ed = 80000;
  let hids = new Array(ed - st + 1).fill(0).map((e, i) => i + st);
  let cs = 50;
  for (let chunk of _.chunk(hids, cs)) {
    await Promise.all(chunk.map((hid) => add_horse_dets_old(hid)));
    // await Promise.all(chunk.map((hid) => add_horse_ratings(hid)));
    console.log("done", chunk.toString());
  }
  console.log("completed");
};

const get_ranks_all_old = async () => {
  let file_path = `${appRootPath}/data/ranks.json`;
  // await init();
  // const docs = await zed_db.db
  //   .collection("rating_blood")
  //   .find({}, { projection: { hid: 1, rank: 1, _id: 0 } })
  //   .toArray();
  // console.log("ranks got: ", docs.length);
  // write_to_path({ file_path, data: docs });

  // let docs = read_from_path({ file_path });
  // let mongo_push = [];
  // for (let { hid, rank } of docs) {
  //   mongo_push.push({
  //     updateOne: {
  //       filter: { hid },
  //       update: { $set: { rank } },
  //       upsert: true,
  //     },
  //   });
  // }
  // console.log(mongo_push.length);
  // await zed_db.db.collection("rating_blood2").bulkWrite(mongo_push);
};

const get_parents_all_old = async () => {
  let file_path = `${appRootPath}/data/parents_d.json`;
  // const docs = await zed_db.db
  //   .collection("rating_blood")
  //   .find(
  //     { hid: { $lt: 80001 } },
  //     {
  //       projection: {
  //         hid: 1,
  //         "details.parents_d.mother.bloodline": 1,
  //         "details.parents_d.mother.breed_type": 1,
  //         "details.parents_d.mother.genotype": 1,
  //         "details.parents_d.mother.horse_type": 1,
  //         "details.parents_d.father.bloodline": 1,
  //         "details.parents_d.father.breed_type": 1,
  //         "details.parents_d.father.genotype": 1,
  //         "details.parents_d.father.horse_type": 1,
  //         _id: 0,
  //       },
  //     }
  //   )
  //   .toArray();
  // write_to_path({ file_path, data: docs });

  let docs = read_from_path({ file_path });
  // docs = docs.slice(4, 5);
  let mgp = [];
  for (let d of docs) {
    let { hid, details = {} } = d;
    let { parents_d } = details;
    if (_.isEmpty(parents_d)) parents_d = { mother: null, father: null };
    mgp.push({
      updateOne: {
        filter: { hid },
        update: { $set: { parents_d } },
        upsert: true,
      },
    });
  }
  console.log(mgp);
  await zed_db.db.collection("horse_details").bulkWrite(mgp);
  await zed_db.db;
  console.log("done");
};

const bulk_write_horse_details = async (obar) => {
  let mgp = [];
  for (let ob of obar) {
    if (_.isEmpty(ob)) continue;
    let { hid } = ob;
    mgp.push({
      updateOne: {
        filter: { hid },
        update: { $set: ob },
        upsert: true,
      },
    });
  }
  await zed_db.db.collection("horse_details").bulkWrite(mgp);
};
const add_horse_dets_old_in_bulk = async () => {
  let st = 59999;
  let ed = 82000;
  let cs = 500;
  let hids = new Array(ed - st + 1).fill(0).map((e, i) => i + st);
  // let hids = [3312, 31896];
  for (let chunk_hids of _.chunk(hids, cs)) {
    let ob = await get_horses_dets_old(chunk_hids);
    await bulk_write_horse_details(ob);
    console.log("done", chunk_hids.toString());
  }
};

const bulk_write_kid_to_parent = async (obar) => {
  let mgp = [];
  for (let ob of obar) {
    if (_.isEmpty(ob)) continue;
    let { hid, parents } = ob;
    if (parents?.mother) {
      mgp.push({
        updateOne: {
          filter: { hid: parents.mother },
          update: { $addToSet: { offsprings: hid } },
          upsert: true,
        },
      });
    }
    if (parents?.father) {
      mgp.push({
        updateOne: {
          filter: { hid: parents.father },
          update: { $addToSet: { offsprings: hid } },
          upsert: true,
        },
      });
    }
  }

  if (!_.isEmpty(mgp))
    await zed_db.db.collection("horse_details").bulkWrite(mgp);
};
const add_hid_to_parents_doc_in_bulk = async () => {
  let st = 1;
  let ed = 82000;
  let cs = 500;
  let hids = new Array(ed - st + 1).fill(0).map((e, i) => i + st);
  // let hids = [31896];
  for (let chunk_hids of _.chunk(hids, cs)) {
    let ob = await zed_db.db
      .collection("horse_details")
      .find(
        { hid: { $in: chunk_hids } },
        {
          projection: {
            _id: 0,
            hid: 1,
            "parents.mother": 1,
            "parents.father": 1,
          },
        }
      )
      .toArray();
    // console.log(ob);
    await bulk_write_kid_to_parent(ob);
    console.log("done", chunk_hids.toString());
  }
};

const fetch_zed_horse_doc = (hid) => {
  hid = parseInt(hid);
  let api = `https://api.zed.run/api/v1/horses/get/${hid}`;
  let ob = fetch_a(api) || null;
  return ob;
};
const struct_zed_horse_doc = ({ hid, doc }) => {
  // console.log(hid, doc);
  hid = parseInt(hid);
  if (_.isEmpty(doc) || doc?.err) return null;
  let {
    bloodline,
    breed_type,
    genotype,
    horse_type,
    class: tc,
    hash_info,
    parents: parents_raw,
    owner_stable_slug: slug,
    rating,
  } = doc;
  let { color, hex_code, name } = hash_info;
  let parents = {
    mother: parents_raw?.mother?.horse_id || null,
    father: parents_raw?.father?.horse_id || null,
  };
  let parents_d = {};
  if (parents.mother) {
    let { bloodline, breed_type, genotype, horse_type } = parents_raw?.mother;
    parents_d.mother = { bloodline, breed_type, genotype, horse_type };
  } else parents_d.mother = null;
  if (parents.father) {
    let { bloodline, breed_type, genotype, horse_type } = parents_raw?.father;
    parents_d.father = { bloodline, breed_type, genotype, horse_type };
  } else parents_d.father = null;
  let ob = {
    hid,
    bloodline,
    breed_type,
    genotype,
    horse_type,
    color,
    hex_code,
    name,
    tc,
    rating,
    slug,
    parents,
    parents_d,
  };
  // console.log(hid, ob);
  return ob;
};
const zed_horse_data_from_api = async (hid) => {
  // return fetch_zed_horse_doc(hid)
  return zedf
    .horse(hid)
    .then((doc) => ({ hid, doc }))
    .then(struct_zed_horse_doc);
};
const add_horse_from_zed_in_bulk = async () => {
  try {
    await init();
    // let doc = await zed_db.db.collection("horse_details").find({}).max({}).toArray();
    // doc_hid = doc[0]?.hid;
    // let st = doc_hid;
    let st = 124000;
    let ed = 128000;
    let cs = 5;
    let hids = new Array(ed - st + 1).fill(0).map((e, i) => i + st);
    // let hids = [82001, 90125];\
    for (let chunk_hids of _.chunk(hids, cs)) {
      let obar = await Promise.all(
        chunk_hids.map((hid) =>
          fetch_zed_horse_doc(hid)
            .then((doc) => ({ hid, doc }))
            .then(struct_zed_horse_doc)
        )
      );
      let mgp = [];
      for (let ob of obar) {
        if (_.isEmpty(ob)) continue;
        let { hid } = ob;
        // console.log(hid, ob);
        mgp.push({
          updateOne: {
            filter: { hid },
            update: { $set: ob },
            upsert: true,
          },
        });
      }
      console.log(mgp.length);
      if (!_.isEmpty(mgp))
        await zed_db.db.collection("horse_details").bulkWrite(mgp);
      await bulk_write_kid_to_parent(obar);
      console.log("done", chunk_hids.toString());
      await delay(3000);
    }
  } catch (err) {
    console.log("err on ", err.message);
  }
};
const add_new_horse_from_zed_in_bulk = async (hids, cs = 5) => {
  // await init();
  for (let chunk_hids of _.chunk(hids, cs)) {
    let obar = await Promise.all(
      chunk_hids.map((hid) =>
        // fetch_zed_horse_doc(hid)
        zedf
          .horse(hid)
          .then((doc) => ({ hid, doc }))
          .then(struct_zed_horse_doc)
      )
    );
    let mgp = [];
    for (let ob of obar) {
      if (_.isEmpty(ob)) continue;
      let { hid } = ob;
      // console.log(hid, ob);
      mgp.push({
        updateOne: {
          filter: { hid },
          update: { $set: ob },
          upsert: true,
        },
      });
    }
    if (!_.isEmpty(mgp))
      await zed_db.db.collection("horse_details").bulkWrite(mgp);
    await bulk_write_kid_to_parent(obar);
    console.log("done", chunk_hids.toString());
    // await delay(3000);
    return _.chain(obar)
      .map((i) => i && i.bloodline && i.hid)
      .compact()
      .value();
  }
};
const missing_zed_horse_tc_update = async () => {
  await init();
  // let docs = await zed_db.db
  //   .collection("horse_details")
  //   .find({ tc: null }, { projection: { tc: 1, hid: 1, _id: 0 } })
  //   .toArray();
  let docs = await zed_db.db
    .collection("rating_blood2")
    .find({ tc: null }, { projection: { _id: 0, hid: 1 } })
    .toArray();

  let hids = _.map(docs, "hid");
  console.log("got missing", hids.length);
  if (_.isEmpty(hids)) return;

  // await add_new_horse_from_zed_in_bulk(hids);
  await odds_generator_for_hids(hids);
  console.log("missing_zed_horse_tc_update");
};

const add_to_new_horses_bucket = async (hids) => {
  let id = "new_horses_bucket";
  hids = _.compact(hids);
  await zed_db.db
    .collection("script")
    .updateOne(
      { id },
      { $set: { id }, $addToSet: { hids: { $each: hids } } },
      { upsert: true }
    );
  console.log("pushed", hids.length, "horses to new_horses_bucket");
};
const rem_from_new_horses_bucket = async (hids) => {
  let id = "new_horses_bucket";
  hids = _.compact(hids);
  await zed_db.db
    .collection("script")
    .updateOne({ id }, { $pullAll: { hids: hids } }, { upsert: true });
  console.log("removed", hids.length, "horses from new_horses_bucket");
};

const zed_horses_needed_bucket_using_hawku = async () => {
  await init();
  while (true) {
    let id = "new_horses_bucket";
    let docs = (await zed_db.db.collection("script").findOne({ id })) || {};
    let { hids = [] } = docs;
    // hids = [126901];
    console.log("new hids:", hids?.length);
    let cs = 2;
    for (let chunk_hids of _.chunk(hids, cs)) {
      await Promise.all(chunk_hids.map((hid) => scrape_horse_details(hid)));
      let hids_ob = _.chain(chunk_hids)
        .map((i) => [i, null])
        .fromPairs()
        .value();
      await update_odds_and_breed_for_race_horses(hids_ob);
      await rem_from_new_horses_bucket(chunk_hids);
      console.log("done", chunk_hids.toString());
      await delay(500);
    }
    console.log("completed zed_horses_needed_bucket_using_hawku ");
    await delay(5000);
  }
};
const zed_horses_needed_manual_using_hawku = async () => {
  await init();
  let end_doc = await zed_db.db
    .collection("horse_details")
    .find({ hid: { $type: 16 } }, { projection: { _id: 0, hid: 1 } })
    .sort({ hid: -1 })
    .limit(1)
    .toArray();
  end_doc = end_doc && end_doc[0];
  let st = end_doc?.hid || 125000;
  st = st - 10000;
  let ed = 200000;
  console.log({ st, ed });
  let cs = 3;
  let hids_all = new Array(ed - st + 1).fill(0).map((ea, idx) => st + idx);
  let continue_thresh = 50;
  let null_resps = 0;
  outer: while (true) {
    let docs_exists =
      (await zed_db.db
        .collection("horse_details")
        .find({ hid: { $gt: st - 1 } }, { projection: { _id: 0, hid: 1 } })
        .toArray()) || {};
    let hids_exists = _.map(docs_exists, "hid");
    // let hids_exists = [];
    let hids = _.difference(hids_all, hids_exists);
    // hids = [126901];
    // console.log("new hids:", hids?.length);

    for (let chunk_hids of _.chunk(hids, cs)) {
      let resps = await Promise.all(
        chunk_hids.map((hid) => scrape_horse_details(hid).then((d) => [hid, d]))
      );
      resps = _.fromPairs(resps);
      let this_resps = _.values(resps);

      if ((this_resps.length = this_resps.filter((e) => e == null).length))
        null_resps += this_resps.length;
      else null_resps = 0;
      if (null_resps >= continue_thresh) {
        console.log("found consec", continue_thresh, "empty horses");
        console.log("continue from start");
        await delay(120000);
        continue outer;
      }

      chunk_hids = _.chain(resps)
        .entries()
        .filter((e) => e[1] !== null)
        .map(0)
        .value();

      let hids_ob = _.chain(chunk_hids)
        .map((i) => [i, null])
        .fromPairs()
        .value();
      await update_odds_and_breed_for_race_horses(hids_ob);
      // await rem_from_new_horses_bucket(chunk_hids);
      console.log("## DONE SCRAPE ", chunk_hids.toString(), "\n");
      await delay(500);
    }
    console.log("completed zed_horses_needed_bucket_using_hawku ");
    await delay(120000);
  }
};

const zed_horses_needed_bucket_using_api = async () => {
  await init();
  while (true) {
    let id = "new_horses_bucket";
    let docs = (await zed_db.db.collection("script").findOne({ id })) || {};
    let { hids = [] } = docs;
    // hids = [126901];
    console.log("new hids:", hids?.length);
    let cs = 2;
    for (let chunk_hids of _.chunk(hids, cs)) {
      let resps = await add_new_horse_from_zed_in_bulk(hids, cs);
      let hids_ob = _.chain(resps)
        .map((i) => [i, null])
        .fromPairs()
        .value();
      await update_odds_and_breed_for_race_horses(hids_ob);
      await rem_from_new_horses_bucket(chunk_hids);
      console.log("done", chunk_hids.toString());
      await delay(500);
    }
    console.log("completed zed_horses_needed_bucket_using_hawku ");
    await delay(5000);
  }
};
const zed_horses_needed_manual_using_api = async () => {
  await init();
  let end_doc = await zed_db.db
    .collection("horse_details")
    .find({ hid: { $type: 16 } }, { projection: { _id: 0, hid: 1 } })
    .sort({ hid: -1 })
    .limit(1)
    .toArray();
  end_doc = end_doc && end_doc[0];
  let st = end_doc?.hid || 1;
  st = st - 15000;
  // st = 128479;
  // let ed = 131000;
  let ed = 200000;
  console.log({ st, ed });
  let cs = 5;
  let hids_all = new Array(ed - st + 1).fill(0).map((ea, idx) => st + idx);
  let continue_thresh = 50;
  let null_resps = 0;
  outer: while (true) {
    let docs_exists =
      (await zed_db.db
        .collection("horse_details")
        .find(
          { hid: { $gt: st - 1 } },
          { projection: { _id: 0, hid: 1, bloodline: 1 } }
        )
        .toArray()) || {};
    let hids_exists = _.map(docs_exists, (i) => {
      if (i?.bloodline) return i.hid;
      return null;
    });

    let hids = _.difference(hids_all, hids_exists);
    console.log("hids.len: ", hids.length);

    for (let chunk_hids of _.chunk(hids, cs)) {
      console.log("GETTING", chunk_hids);
      let resps = await add_new_horse_from_zed_in_bulk(chunk_hids, cs);

      if (resps?.length == 0) {
        console.log("found consec", chunk_hids.length, "empty horses");
        console.log("continue from start after 5 minutes");
        await delay(300000);
        continue outer;
      }
      console.log("wrote", resps.length, "to horse_details");

      chunk_hids = resps;

      let hids_ob = _.chain(chunk_hids)
        .map((i) => [i, null])
        .fromPairs()
        .value();
      await update_odds_and_breed_for_race_horses(hids_ob);
      // await rem_from_new_horses_bucket(chunk_hids);
      await fix_horse_type_using_kid_ids(chunk_hids);
      console.log("## DONE SCRAPE ", chunk_hids.toString(), "\n");
      // await delay(2000);
    }
    console.log("completed zed_horses_needed_bucket_using_zed_api ");
    await delay(120000);
  }
};

const zed_horses_racing_update_odds = async () => {
  await init();
  let cs = 10;
  outer: while (true) {
    let hids =
      (await zed_db.db.collection("script").findOne({ id: "racing_horses" })) ||
      [];
    if (!_.isEmpty(hids)) hids = hids?.racing_horses || [];
    for (let chunk_hids of _.chunk(hids, cs)) {
      let ob = _.fromPairs(chunk_hids);
      await update_odds_and_breed_for_race_horses(ob);
      await zed_db.db.collection("script").updateOne(
        { id: "racing_horses" },
        {
          $pullAll: { racing_horses: chunk_hids },
        },
        { upsert: true }
      );
    }
    console.log("-------");
    delay(1000);
  }
};
// zed_horses_racing_update_odds();

const zed_horses_fix_unnamed_foal = async () => {
  await init();
  let cs = 10;

  let end_doc = await zed_db.db
    .collection("horse_details")
    .find({ hid: { $type: 16 } }, { projection: { _id: 0, hid: 1 } })
    .sort({ hid: -1 })
    .limit(1)
    .toArray();
  end_doc = end_doc && end_doc[0];
  // let st = end_doc?.hid || 1;
  // st = st - 50000;

  let st = 1;

  let docs = await zed_db.db
    .collection("horse_details")
    .find(
      { hid: { $gt: st }, name: "Unnamed Foal" },
      { projection: { hid: 1, _id: 1 } }
    )
    .toArray();
  let hids = _.map(docs, "hid");
  console.log("got", hids.length, "Unnamed Foal");
  for (let chunk of _.chunk(hids, cs)) {
    let data = await Promise.all(
      chunk.map((hid) => zed_horse_data_from_api(hid))
    );
    console.log(chunk.toString());
    data = _.chain(data)
      .compact()
      .map((i) => {
        let { hid, name } = i;
        if (name == "Unnamed Foal") return null;
        return [hid, name];
      })
      .compact()
      .value();

    let bulk = [];
    for (let [hid, name] of data) {
      bulk.push({
        updateOne: {
          filter: { hid },
          update: { $set: { name } },
        },
      });
    }
    if (!_.isEmpty(bulk))
      await zed_db.db.collection("horse_details").bulkWrite(bulk);
    console.log("named:", data.length, _.map(data, 1).toString());
  }
  console.log("completed zed_horses_fix_unnamed_foal");
};
const zed_horses_fix_unnamed_foal_cron = async () => {
  let runner = zed_horses_fix_unnamed_foal;
  let cron_str = "0 */3 * * *";
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("Next run:", c_itvl.next().toISOString(), "\n");
  cron.schedule(cron_str, () => runner(), { scheduled: true });
};
// zed_horses_fix_unnamed_foal();

const get_ed_horse = async () => {
  let end_doc = await zed_db.db
    .collection("horse_details")
    .find({ hid: { $type: 16 } }, { projection: { _id: 0, hid: 1 } })
    .sort({ hid: -1 })
    .limit(1)
    .toArray();
  end_doc = end_doc && end_doc[0];
  return end_doc?.hid;
};

module.exports = {
  zed_horses_all_scrape,
  add_horse_from_zed_in_bulk,
  add_new_horse_from_zed_in_bulk,
  missing_zed_horse_tc_update,
  add_to_new_horses_bucket,
  rem_from_new_horses_bucket,
  zed_horses_needed_bucket_using_hawku,
  zed_horses_needed_manual_using_hawku,
  zed_horses_needed_bucket_using_api,
  zed_horses_needed_manual_using_api,
  zed_horses_racing_update_odds,
  zed_horses_fix_unnamed_foal,
  zed_horses_fix_unnamed_foal_cron,
  get_ed_horse,
};
