const { zed_db, init } = require("../connection/mongo_connect");
const _ = require("lodash");
const zedf = require("../utils/zedf");
const cron_parser = require("cron-parser");
const cron = require("node-cron");
const parents = require("./parents");
const bulk = require("../utils/bulk");
const { get_ed_horse } = require("../utils/cyclic_dependency");
const mega = require("./mega");
const { delay } = require("../utils/utils");
const ancestry = require("./ancestry");
const utils = require("../utils/utils");
const cyclic_depedency = require("../utils/cyclic_dependency");

const def_cs = 30;

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
  };
  // console.log(hid, ob);
  return ob;
};
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

const get_new = async () => {
  let st = await get_ed_horse();
  console.log("last:", st);
  st = st - 15000;
  let ed = st * 2;
  console.log({ st, ed });
  let cs = def_cs;
  let hids_all = new Array(ed - st + 1).fill(0).map((ea, idx) => st + idx);

  let fail_count = 0;

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
        fail_count++;
        if (fail_count > 10) {
          console.log("found consec", chunk_hids.length, "empty horses");
          console.log("continue from start after 5 minutes");
          await delay(60 * 1000);
          continue outer;
        } else {
          continue;
        }
      } else fail_count = 0;
      console.log("wrote", resps.length, "to horse_details");

      chunk_hids = _.map(resps, "hid");
      await mega.only_w_parents_br(chunk_hids);
      await parents.fix_horse_type_using_kid_ids(chunk_hids);
      await ancestry.only(chunk_hids);
      console.log("## GOT ", chunk_hids.toString(), "\n");
    }
    console.log("completed zed_horses_needed_bucket_using_zed_api ");
    await delay(60 * 1000);
  }
};
const get_only = async (hids) => {
  let cs = def_cs;
  let hids_all = hids.map((h) => parseInt(h));
  console.log("hids_all", hids_all.length);
  for (let chunk_hids of _.chunk(hids_all, cs)) {
    console.log("GETTING", chunk_hids);
    let resps = await add_hdocs(chunk_hids, cs);
    await delay(100);
    if (resps?.length == 0) {
      console.log("break");
      break;
    }
    console.log("wrote", resps.length, "to horse_details");

    chunk_hids = _.map(resps, "hid");
    await mega.only_w_parents_br(chunk_hids);
    await parents.fix_horse_type_using_kid_ids(chunk_hids);
    await ancestry.only(chunk_hids);
    console.log("## GOT ", chunk_hids.toString(), "\n");
  }
};
const get_range = async (range) => {
  let [st, ed] = range;
  st = utils.get_n(st);
  ed = utils.get_n(ed);
  if (ed == "ed" || ed == null) ed = await get_ed_horse();
  let cs = def_cs;
  let hids_all = new Array(ed - st + 1).fill(0).map((ea, idx) => st + idx);
  console.log([st, ed]);
  await get_only(hids_all);
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

const get_missings = async (range) => {
  let [st, ed] = range;
  st = utils.get_n(st);
  ed = utils.get_n(ed);
  if (ed == "ed" || ed == null) ed = await get_ed_horse();
  let cs = def_cs;
  let hids_all = new Array(ed - st + 1).fill(0).map((ea, idx) => st + idx);
  for (let chunk_hids of _.chunk(hids_all, 100)) {
    let [a, b] = [chunk_hids[0], chunk_hids[chunk_hids.length - 1]];
    console.log("checking", a, "->", b);
    let hids1 = await get_valid_hids_in_coll(chunk_hids, "horse_details");
    let hids2 = await get_valid_hids_in_coll(chunk_hids, "rating_blood3");
    let hids3 = await get_valid_hids_in_coll(chunk_hids, "rating_breed3");
    let hids4 = await get_valid_hids_in_coll(chunk_hids, "rating_flames3");
    let hids5 = await get_valid_hids_in_ancestry(chunk_hids);

    console.log("hids1:", hids1.length);
    console.log("hids2:", hids2.length);
    console.log("hids3:", hids3.length);
    console.log("hids4:", hids4.length);
    console.log("hids5:", hids5.length);

    let hids_exists = _.intersection(hids1, hids2, hids3, hids4, hids5);
    console.log("hids_exists", hids_exists.length);
    let missings = _.difference(chunk_hids, hids_exists);
    console.log("missings", missings.length, missings);
    if (_.isEmpty(missings)) continue;
    await get_only(missings);
  }
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
    if (resps?.length == 0) {
      console.log("break");
      break;
    }
  }
  console.log("end");
};
const get_range_hdocs = async (range) => {
  let [st, ed] = range;
  st = utils.get_n(st);
  ed = utils.get_n(ed);
  if (ed == "ed" || ed == null) ed = await get_ed_horse();
  let cs = def_cs;
  let hids_all = new Array(ed - st + 1).fill(0).map((ea, idx) => st + idx);
  console.log([st, ed]);
  await get_only_hdocs(hids_all);
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
  let runner = zed_horses_fix_unnamed_foal;
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
};

module.exports = horses;
