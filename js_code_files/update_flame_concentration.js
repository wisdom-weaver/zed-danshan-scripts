const { init, zed_db, zed_ch } = require("./index-run");
const _ = require("lodash");
const { download_eth_prices, get_fee_cat_on } = require("./base");
const { ObjectID, ObjectId } = require("mongodb");
const { delay } = require("./utils");
const { get_max_horse } = require("./max_horses");
const { get_races_of_hid } = require("./cyclic_dependency")

const add_fee_cat_to_doc = async (doc) => {
  let _id = doc._id;
  if (doc[14] != null) {
    // console.log("skip", doc[14]);
    return;
  }
  _id = ObjectId(_id);
  let rid = doc[4];
  let fee = doc[3];
  let date = doc[2];
  let fee_cat = get_fee_cat_on({ date, fee });
  // console.log(_id, fee_cat);
  await zed_ch.collection("zed").updateOne({ _id }, { $set: { 14: fee_cat } });
};
const add_fee_cat_to_hid_races = async (hid) => {
  let docs = await zed_ch.db
    .collection("zed")
    .find({
      6: hid,
      14: { $exists: false },
    })
    .toArray();
  // console.table(docs);
  let ids = _.map(docs, "_id").map((e) => ObjectId(e));
  // console.log(ids);
  let cs = 50;
  let n = docs.length;
  for (let chunk of _.chunk(docs, 50))
    await Promise.all(chunk.map((doc) => add_fee_cat_to_doc(doc)));
};
const each_horse_fee_cat_n_flames_rating = async (hid) => {
  try {
    await add_fee_cat_to_hid_races(hid);
    await get_n_upload_rating_flames(hid);
  } catch (err) {
    console.log("err on ", hid, "continuing");
  }
};

const add_fee_cat_to_all_horses_races = async () => {
  await init();
  await download_eth_prices();
  let st = 23720;
  let ed = await get_max_horse();
  console.log(st, ":", ed);
  let hids = new Array(ed - st + 1).fill(0).map((e, i) => st + i);
  // hids = [37136, 53814, 75441, 3312, 19587];
  // hids = [75441];
  let cs = 20;
  for (let chunk of _.chunk(hids, cs)) {
    await Promise.all(chunk.map((hid) => add_fee_cat_to_hid_races(hid)));
    console.log(chunk[0], " -> ", chunk[chunk.length - 1]);
  }
};

let keys = (() => {
  let classes = [1, 2, 3, 4, 5];
  let dists = [1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600];
  let fee_cats = ["A", "B", "C"];
  let keys = [];
  for (let c of classes)
    for (let f of fee_cats) for (let d of dists) keys.push(`${c}${f}${d}`);
  return keys;
})();

const get_points_from_place = (p) => {
  p = parseInt(p);
  switch (p) {
    case 1:
      return 4;
    case 2:
      return 3;
    case 3:
      return 2;
    case 4:
      return 0;
    case 5:
      return -1;
    case 6:
      return -2;
    case 7:
      return -3;
    case 8:
      return -4;
    case 9:
      return 0;
    case 10:
      return 1;
    case 11:
      return 2;
    case 12:
      return 3;
    default:
      0;
  }
};

const def_rating_flames = {
  key: null,
  races_n: null,
  cfd_n: null,
  flames_n: null,
  flames_per: null,
  avg_points: null,
};
const get_rating_flames = async (hid) => {
  try {
    hid = parseInt(hid);
    let races = await get_races_of_hid(hid);
    if (races?.length == 0) return { hid, ...def_rating_flames, races_n: 0 };
    // console.table(races);
    let races_n = races.length;
    let ob = {};
    for (let key of keys) {
      let c = parseInt(key[0]);
      let f = key[1];
      let F = 1;
      let d = parseInt(key.slice(2));
      let fr = races.filter((r) => {
        let fee_cat = get_fee_cat_on({ date: r.date, fee: r.entryfee });
        if (r.thisclass !== c) return false;
        // if (r.fee_cat !== f) return false;
        if (fee_cat !== f) return false;
        if (r.distance !== d) return false;
        return true;
      });
      let fr_F = fr.filter((r) => r.flame == F);
      if (fr_F.length <= 2) continue;

      let points_ar = fr_F.map((e) => get_points_from_place(e.place));
      let tot_points = _.sum(points_ar);
      let avg_points = _.mean(points_ar);
      // console.log(key, avg_points)
      ob[key] = {
        cf: `${c}${f}`,
        races_n,
        cfd_n: fr.length,
        flames_n: fr_F.length,
        tot_points,
        avg_points,
      };
    }
    ob = _.entries(ob).map(([key, e]) => ({
      key,
      ...e,
    }));
    ob = ob.map((ea) => {
      let flames_per = (ea.flames_n * 100) / ea.cfd_n;
      flames_per = parseFloat(flames_per).toFixed(2);
      flames_per = parseFloat(flames_per);

      return { ...ea, flames_per };
    });
    ob = _.orderBy(
      ob,
      ["cf", "flames_per", "avg_points"],
      ["asc", "desc", "desc"]
    );

    if (_.isEmpty(ob)) return { ...def_rating_flames, hid, races_n };
    // console.table(ob);
    let mx_ob = ob[0];
    // console.table(mx_ob);
    return mx_ob;
  } catch (err) {
    console.log("err on get_rating_flames", hid);
    console.log(err);
  }
};
const get_n_upload_rating_flames = async (hid) => {
  hid = parseInt(hid);
  if (_.isNaN(hid)) return;
  let ob = await get_rating_flames(hid);
  if (_.isEmpty(ob)) return;
  // console.log(hid, ob);
  await zed_db
    .collection("rating_flames")
    .updateOne({ hid }, { $set: ob }, { upsert: true });
};
// add_fee_cat_to_all_horses_races();

const test = async () => {
  await init();
  let hid = 3312;
  await get_n_upload_rating_flames(hid);
  console.log("completed");
};
// test();

const add_rating_flames_to_all_horses_races = async () => {
  await init();
  await download_eth_prices();
  let st = 1;
  let ed = await get_max_horse();
  console.log(st, ":", ed);
  let hids = new Array(ed - st + 1).fill(0).map((e, i) => st + i);
  // hids = [37136, 53814, 75441, 3312, 19587];
  // hids = [75441];
  let cs = 50;
  for (let chunk of _.chunk(hids, cs)) {
    await Promise.all(chunk.map((hid) => get_n_upload_rating_flames(hid)));
    console.log(chunk[0], " -> ", chunk[chunk.length - 1]);
  }
};
// add_rating_flames_to_all_horses_races();

module.exports = {
  get_n_upload_rating_flames,
};
