const { zed_db, init } = require("../connection/mongo_connect");
const _ = require("lodash");
const zedf = require("../utils/zedf");
const cron_parser = require("cron-parser");
const cron = require("node-cron");
const parents = require("./parents");
const bulk = require("../utils/bulk");
const { get_ed_horse, get_range_hids } = require("../utils/cyclic_dependency");
const mega = require("./mega");
const { delay, get_hids } = require("../utils/utils");
const ancestry = require("./ancestry");
const utils = require("../utils/utils");
const cyclic_depedency = require("../utils/cyclic_dependency");
const rating_blood_S = require("./rating_blood");
const mega2 = require("./mega2");

const def_cs = 10;

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
const struct_zed_hdoc = (hid, doc) => {
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
    tx_date,
  } = doc;
  let oid = doc.owner;
  let stable_name = doc.owner_stable;
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
    oid,
    stable_name,
    parents,
    parents_d,
    tx_date,
  };
  // console.log(hid, ob);
  return ob;
};

const get_hdoc = (hid) =>
  zedf.horse(hid).then((doc) => struct_zed_hdoc(hid, doc));

const add_hdocs = async (hids, cs = def_cs) => {
  for (let chunk_hids of _.chunk(hids, cs)) {
    let obar = await Promise.all(
      chunk_hids.map((hid) =>
        zedf.horse(hid).then((doc) => struct_zed_hdoc(hid, doc))
      )
    );
    obar = _.compact(obar);
    bulk.push_bulk("horse_details", obar, "new_horses");
    await bulk_write_kid_to_parent(obar);
    console.log("done", chunk_hids.toString());

    return _.chain(obar)
      .map((i) => (i && i.bloodline ? { hid: i.hid, tc: i.tc } : null))
      .compact()
      .value();
  }
};

const fixer = async () => {
  s_start: while (true) {
    console.log("get_new");
    let back = 50000;
    // let hids = await cyclic_depedency.get_all_hids();
    // hids = hids.slice(hids.length - back);
    let ed = await cyclic_depedency.get_ed_horse();
    let st = 0;
    let now = st;
    let cs = 10;
    let max_fail = 100;
    let fail = max_fail;
    do {
      let [now_st, now_ed] = [now, now + cs - 1];
      let resp = await get_missings([now_st, now_ed], 0);
      now += cs;
      console.log("CROSSED");
    } while (now <= ed);
    console.log("REACHED END");
    console.log("====\n===\n===\n");
    await delay(10 * 60 * 1000);
    console.log("====\n===\n===\n");
    continue s_start;
  }
  // await mega.only_w_parents_br(chunk_hids);
  // await parents.fix_horse_type_using_kid_ids(chunk_hids);
};

const roster_api = (offset = 0) =>
  `https://api.zed.run/api/v1/horses/roster?offset=${offset}&gen[]=1&gen[]=268&horse_name=&sort_by=created_by_desc`;
const get_rosters = async () => {
  let mxnhids = 100;
  let o = 0;
  let hids = [];
  do {
    let nhids = (await zedf.get(roster_api(o))) ?? [];
    nhids = _.map(nhids, "horse_id");
    console.log("offset:", o, nhids.length);
    if (nhids.length == 0) break;
    hids = [...hids, ...nhids];
    o += nhids.length;
  } while (hids.length <= mxnhids);
  return hids;
};
const get_new = async () => {
  console.log("get_new");
  while (true) {
    let nhids = await get_rosters();
    let xhids = await get_valid_hids_in_details(nhids);
    let hids = _.difference(nhids, xhids);
    console.log("listing:", nhids.length);
    console.log("exists :", xhids.length);
    console.log("new    :", hids.length, "\n");
    if (!_.isEmpty(hids)) {
      await get_only(hids, 1);
      console.log("\n\nGOT:", hids.length, "\n");
    }
    console.log("=====\nstarting again in 1 minute....");
    await delay(60 * 1000);
  }
};

const get_only = async (hids, p = 1) => {
  let cs = def_cs;
  let hids_all = hids.map((h) => parseInt(h));
  if (p) console.log("hids_all", hids_all.length);
  let fet = [];
  for (let chunk_hids of _.chunk(hids_all, cs)) {
    if (p) console.log("GETTING", chunk_hids);
    let resps = await add_hdocs(chunk_hids, cs);
    await delay(100);
    if (resps?.length == 0) {
      if (p) console.log("break");
      break;
    }
    if (p) console.log("wrote", resps.length, "to horse_details");

    chunk_hids = _.map(resps, "hid");
    await mega.only_w_parents_br(chunk_hids);
    await parents.fix_horse_type_using_kid_ids(chunk_hids);
    await ancestry.only(chunk_hids);
    if (p) console.log("## GOT ", chunk_hids.toString(), "\n\n");
    fet = [...fet, ...(chunk_hids || [])];
  }
  return fet;
};
const get_only2 = async (hids, p = 1) => {
  let cs = def_cs;
  let hids_all = hids.map((h) => parseInt(h));
  if (p) console.log("hids_all", hids_all.length);
  let fet = [];
  for (let chunk_hids of _.chunk(hids_all, cs)) {
    if (p) console.log("GETTING", chunk_hids);
    // let resps = await add_hdocs(chunk_hids, cs);
    // await delay(100);
    // if (resps?.length == 0) {
    //   if (p) console.log("break");
    //   break;
    // }
    // if (p) console.log("wrote", resps.length, "to horse_details");

    // chunk_hids = _.map(resps, "hid");
    await mega2.only(chunk_hids);
    // await parents.fix_horse_type_using_kid_ids(chunk_hids);
    // await ancestry.only(chunk_hids);
    if (p) console.log("## GOT ", chunk_hids.toString(), "\n\n");
    fet = [...fet, ...(chunk_hids || [])];
  }
  return fet;
};
const get_range = async (range) => {
  let [st, ed] = range;
  st = utils.get_n(st);
  ed = utils.get_n(ed);
  if (ed == "ed" || ed == null) ed = await get_ed_horse();
  let cs = def_cs;
  console.log([st, ed]);
  for (let i = st; i <= ed; i += cs) {
    console.log(i, i + cs);
    let hids = get_hids(i, i + cs);
    await get_only(hids);
  }
};

const get_valid_hids_in_details = async (hids) => {
  let hids5 = await zed_db.db
    .collection("horse_details")
    .find(
      {
        hid: { $in: hids },
        // name: { $ne: "Unnamed Foal" },
      },
      { projection: { _id: 1, hid: 1, bloodline: 1 } }
    )
    .toArray();
  hids5 = hids5.map((h) => (!h.bloodline ? null : h.hid));
  hids5 = _.compact(hids5);
  return hids5;
};
const get_valid_hids_in_coll = async (hids, coll) => {
  let ar = await zed_db.db
    .collection(coll)
    .find({ hid: { $in: hids } }, { projection: { _id: 1, hid: 1 } })
    .toArray();
  return _.compact(_.map(ar, "hid"));
};

const get_valid_hids_in_ancestry = async (hids) => {
  let hids5 = await zed_db.db
    .collection("horse_details")
    .find(
      { hid: { $in: hids } },
      { projection: { _id: 1, hid: 1, ancestry: 1 } }
    )
    .toArray();
  hids5 = hids5.map((h) => (_.isEmpty(h.ancestry) ? null : h.hid));
  hids5 = _.compact(hids5);
  return hids5;
};
const get_valid_hids_in_blood = async (hids) => {
  let hids5 = await zed_db.db
    .collection("rating_blood3")
    .find(
      {
        hid: { $in: hids },
        base_ability: { $exists: true },
        races_n: { $exists: true },
      },
      {
        projection: {
          _id: 0,
          hid: 1,
        },
      }
    )
    .toArray();
  hids5 = hids5.map((h) => h.hid);
  hids5 = _.compact(hids5);
  return hids5;
};

const get_missing_only = async (hids_all, p = 1) => {
  let fet = [];
  let cs = def_cs;
  for (let chunk_hids of _.chunk(hids_all, cs)) {
    let [a, b] = [chunk_hids[0], chunk_hids[chunk_hids.length - 1]];
    console.log("checking", a, "->", b);
    let hids1 = await get_valid_hids_in_details(chunk_hids);
    let hids2 = await get_valid_hids_in_blood(chunk_hids);
    let hids3 = await get_valid_hids_in_coll(chunk_hids, "rating_breed3");
    let hids4 = await get_valid_hids_in_coll(chunk_hids, "rating_flames3");
    let hids5 = await get_valid_hids_in_ancestry(chunk_hids);

    if (p) console.log("hids1:", hids1.length);
    if (p) console.log("hids2:", hids2.length);
    if (p) console.log("hids3:", hids3.length);
    if (p) console.log("hids4:", hids4.length);
    if (p) console.log("hids5:", hids5.length);

    let hids_exists = _.intersection(hids1, hids2, hids3, hids4, hids5);
    if (p) console.log("hids_exists", hids_exists.length);
    let missings = _.difference(chunk_hids, hids_exists);
    if (p) console.log("missings", missings.length, missings);
    else {
      let [a, b] = [missings[0], missings[missings.length - 1]];
      console.log("missings", missings.length, a ? `${a} -> ${b}` : "");
    }
    if (_.isEmpty(missings)) continue;
    let got = await get_only(missings, p);
    // let got = await get_only2(missings, p);
    console.log("#GOT", got.length, "\n\n");
    fet = [...fet, ...(got || [])];
  }
  return fet;
};

const get_missings = async (range, p = 1) => {
  let [st, ed] = range;
  st = utils.get_n(st);
  ed = utils.get_n(ed);
  if (ed == "ed" || ed == null) ed = await get_ed_horse();
  let cs = 10;
  let hids_all = new Array(ed - st + 1).fill(0).map((ea, idx) => st + idx);
  let fet = [];
  await get_missing_only(hids_all);
  return fet;
};

const get_new_hdocs = async () => {
  let st = await get_ed_horse();
  console.log("last:", st);
  st = st - 15000;
  let ed = st * 2;
  console.log({ st, ed });
  let cs = def_cs;
  let hids_all = new Array(ed - st + 1).fill(0).map((ea, idx) => st + idx);

  outer: while (true) {
    let docs_exists1 =
      (await zed_db.db
        .collection("horse_details")
        .find(
          { hid: { $gt: st - 1 } },
          { projection: { _id: 0, hid: 1, bloodline: 1 } }
        )
        .toArray()) || {};
    let docs_exists2 =
      (await zed_db.db
        .collection("rating_blood3")
        .find({ hid: { $gt: st - 1 } }, { projection: { _id: 0, hid: 1 } })
        .toArray()) || {};

    let hids_exists1 = _.map(docs_exists1, (i) => {
      if (i?.bloodline) return i.hid;
      return null;
    });
    let hids_exists2 = _.map(docs_exists2, "hid");
    let hids_exists = _.intersection(hids_exists1, hids_exists2);

    let hids = _.difference(hids_all, hids_exists);
    console.log("hids.len: ", hids.length);

    for (let chunk_hids of _.chunk(hids, cs)) {
      console.log("GETTING", chunk_hids);
      let resps = await add_hdocs(chunk_hids, cs);
      await delay(100);
      if (resps?.length == 0) {
        console.log("found consec", chunk_hids.length, "empty horses");
        console.log("continue from start after 5 minutes");
        await delay(300000);
        continue outer;
      }
      console.log("wrote", resps.length, "to horse_details");
    }
    console.log("completed zed_horses_needed_bucket_using_zed_api ");
    await delay(120000);
  }
};
const get_only_hdocs = async (hids) => {
  let cs = def_cs;
  let hids_all = hids.map((h) => parseInt(h));
  console.log("hids_all", hids_all.length);
  for (let chunk_hids of _.chunk(hids_all, cs)) {
    console.log("GETTING", chunk_hids);
    let resps = await add_hdocs(chunk_hids, cs);
    await delay(100);
    // if (resps?.length == 0) {
    //   console.log("break");
    //   break;
    // }
  }
  console.log("end");
};
const get_range_hdocs = async (range) => {
  let [st, ed] = range;
  st = utils.get_n(st);
  ed = utils.get_n(ed);
  if (ed == "ed" || ed == null) ed = await get_ed_horse();
  let cs = def_cs;
  console.log([st, ed]);
  for (let i = st; i <= ed; i += cs) {
    let hids = get_range_hids(i, i + cs - 1);
    await get_only_hdocs(hids);
  }
};

const fix_unnamed = async () => {
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
  get_only_hdocs(hids);
  console.log("completed zed_horses_fix_unnamed_foal");
};
const fix_unnamed_cron = async () => {
  let runner = fix_unnamed;
  let cron_str = "0 */3 * * *";
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("Next run:", c_itvl.next().toISOString(), "\n");
  cron.schedule(cron_str, () => runner(), { scheduled: true });
};

const fix_stable_h1 = async (hid) => {
  hid = parseInt(hid);
  let doc = await zedf.horse(hid);
  // let name = doc.name;
  let oid = doc.owner;
  let stable_name = doc.owner_stable;
  let slug = doc.owner_stable_slug;
  return { hid, oid, stable_name, slug };
};

const fix_stable = () =>
  bulk.run_bulk_all("stable", fix_stable_h1, "horse_details", def_cs, 0);

const fix_stable_cron = () => {
  let cron_str = "0 0 */3 * *";
  const runner = fix_stable;
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("Next run:", c_itvl.next().toISOString(), "\n");
  cron.schedule(cron_str, () => runner(), { scheduled: true });
};

const delete_only = async (hids) => {
  for (let hid of hids) {
    hid = parseInt(hid);
    console.log(hid);
    await zed_db.db.collection("horse_details").deleteOne({ hid });
    await zed_db.db.collection("rating_blood3").deleteOne({ hid });
    await zed_db.db.collection("rating_breed3").deleteOne({ hid });
    await zed_db.db.collection("rating_fllames3").deleteOne({ hid });
    await zed_db.db.collection("gap4").deleteOne({ hid });
    await zed_db.db.collection("dp4").deleteOne({ hid });
    console.log("deleted");
  }
};

const horses = {
  get_new,
  get_only,
  get_range,
  fix_unnamed,
  fix_unnamed_cron,
  fix_stable,
  fix_stable_cron,
  get_new_hdocs,
  get_only_hdocs,
  get_range_hdocs,
  get_missings,
  delete_only,
  fixer,
  struct_zed_hdoc,
  get_hdoc,
};

module.exports = horses;
