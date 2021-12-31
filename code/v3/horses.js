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

const def_cs = 15;

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
      let resps = await add_hdocs(chunk_hids, cs);

      if (resps?.length == 0) {
        console.log("found consec", chunk_hids.length, "empty horses");
        console.log("continue from start after 5 minutes");
        await delay(300000);
        continue outer;
      }
      console.log("wrote", resps.length, "to horse_details");

      chunk_hids = _.map(resps, "hid");
      await mega.only(chunk_hids);
      await parents.fix_horse_type_using_kid_ids(chunk_hids);
      console.log("## GOT ", chunk_hids.toString(), "\n");
    }
    console.log("completed zed_horses_needed_bucket_using_zed_api ");
    await delay(120000);
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
const fix_unnamed_cron = async () => {
  let runner = zed_horses_fix_unnamed_foal;
  let cron_str = "0 */3 * * *";
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("Next run:", c_itvl.next().toISOString(), "\n");
  cron.schedule(cron_str, () => runner(), { scheduled: true });
};

const horses = {
  get_new,
  fix_unnamed,
  fix_unnamed_cron,
};

module.exports = horses;
