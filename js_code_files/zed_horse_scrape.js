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
const { delay } = require("./utils");

// X-paths
// name            /html/body/ul/div/main/main/div[1]/div[1]/div[2]/h1
// dets            /html/body/ul/div/main/main/div[1]/div[1]/div[2]/p[1]/span
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
    await driver.get(url);
    await driver.sleep(3000);
    let ob = { hid };
    let page = await elem_by_x(driver, base_x);
    ob.name = await extract_name(page);
    let dets = await extract_dets(page);
    let own = await extract_owner(page);
    ob = { ...ob, ...dets, ...own };
    await click_fam_btn(page);
    let fam = await get_fam_part(page);
    ob = { ...ob, ...fam };
    if (driver && driver.quit) await driver.quit();
    return ob;
  } catch (err) {
    console.log("err on scrape_horse_details", hid);
    console.log("err on scrape_horse_details", err);
    if (driver && driver.quit) await driver.quit();
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
  let st = 2750;
  let ed = 111000;
  let hids = new Array(ed - st + 1).fill(0).map((e, i) => i + st);
  let cs = 8;
  for (let chunk of _.chunk(hids, cs)) {
    await Promise.all(chunk.map((hid) => get_n_upload_horse_details(hid)));
    console.log("done", chunk.toString());
  }
  console.log("completed");
};

const zed_add_horses_from_old_collection = async () => {
  await init();
  console.log("started");
  let st = 2750;
  let ed = 111000;
  let hids = new Array(ed - st + 1).fill(0).map((e, i) => i + st);
  let cs = 8;
  for (let chunk of _.chunk(hids, cs)) {
    await Promise.all(chunk.map((hid) => get_n_upload_horse_details(hid)));
    console.log("done", chunk.toString());
  }
  console.log("completed");
};

const runner = async () => {
  // await init();
  // console.log("runner");
  // await driver_test();
  // let hid = 48992;
  // await console.log(ob);
  // console.log("done");
  await zed_horses_all_scrape();
  // await init();
  // let doc = await zed_db.db.collection("horse_details").findOne({ hid: 92597 });
  // console.log(doc);
};
// runner();

module.exports = {
  zed_horses_all_scrape,
};
