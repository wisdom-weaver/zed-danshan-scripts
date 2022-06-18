const {
  get_fee_cat_on,
  download_eth_prices,
  get_entryfee_usd,
} = require("./base");
const _ = require("lodash");
const { zed_ch, init, zed_db } = require("../connection/mongo_connect");
const { get_fee_tag } = require("./utils");
const { knex_conn } = require("../connection/knex_connect");
const cron_parser = require("cron-parser");
const zedf = require("./zedf");
const cronstrue = require("cronstrue");
const v5_conf = require("../v5/v5_conf");
const moment = require("moment");

const key_mapping_bs_zed = [
  ["_id", "_id"],
  ["1", "distance"],
  ["2", "date"],
  ["3", "entryfee"],
  ["4", "raceid"],
  ["5", "thisclass"],
  ["6", "hid"],
  ["7", "finishtime"],
  ["8", "place"],
  ["9", "name"],
  ["10", "gate"],
  ["11", "odds"],
  ["12", "unknown"],
  ["13", "flame"],
  ["14", "fee_cat"],
  ["15", "adjfinishtime"],
  ["16", "htc"],
  ["17", "race_name"],
  ["18", "entryfee_usd"],
  ["19", "fee_tag"],
  ["20", "prize"],
  ["21", "prize_usd"],
  ["22", "hrating"],
  ["23", "adjtime"],
  ["24", "speed"],
  ["25", "speed_rat"],
];

const from_ch_zed_collection = async (query) => {
  try {
    let data = await zed_ch.db.collection("zed").find(query).toArray();
    data = _.uniqBy(data, (i) => [i["4"], i["6"]].join());
    return data;
  } catch (err) {
    return [];
  }
};

const get_tunnel = (dist) => {
  if (dist >= 1000 && dist <= 1400) return "S";
  if (dist >= 1600 && dist <= 2000) return "M";
  if (dist >= 2200 && dist <= 2600) return "D";
  return null;
};

const struct_race_row_data = (data) => {
  try {
    // console.log(data.length);
    if (_.isEmpty(data)) return [];
    data = data?.map((row) => {
      // console.log(row);
      if (row == null) return null;
      return key_mapping_bs_zed.reduce(
        (acc, [key_init, key_final]) => ({
          ...acc,
          [key_final]: row[key_init] || 0,
        }),
        {}
      );
    });
    data = _.compact(data);
    data = data.map((e) => {
      let { entryfee: fee, date } = e;
      let entryfee_usd = get_entryfee_usd({ fee, date });
      let fee_tag = get_fee_tag(entryfee_usd);
      let tunnel = get_tunnel(e.distance);
      return { ...e, entryfee_usd, fee_tag, tunnel };
    });
  } catch (err) {
    if (data.name == "MongoNetworkError") {
      console.log("MongoNetworkError");
    }
    return [];
  }
  return data;
};

const get_races_of_hid = async (hid) => {
  hid = parseInt(hid);
  if (isNaN(hid)) return [];
  hid = parseInt(hid);
  let query = { 6: hid };
  let data = await from_ch_zed_collection(query);
  data = struct_race_row_data(data);
  return data;
};

const progress_bar = (a, b) => {
  let len = 50;
  let per = parseFloat((a / b) * 100).toFixed(2);
  let eqs = new Array(Math.ceil((len * a) / b)).fill("=").join("");
  let dts = new Array(Math.ceil(len * (1 - a / b))).fill(".").join("");
  return `[${eqs}>${dts}] ${per}%| ${a}/${b}`;
};

const initiate = async () => {
  await init();
  await download_eth_prices();
};

const general_bulk_push = async (coll, obar) => {
  try {
    if (_.isEmpty(obar)) return;
    let bulk = [];
    for (let ob of obar) {
      if (_.isEmpty(ob)) continue;
      let { hid } = ob;
      bulk.push({
        updateOne: {
          filter: { hid },
          update: { $set: ob },
          upsert: true,
        },
      });
    }
    await zed_db.db.collection(coll).bulkWrite(bulk);
    let len = obar.length;
    let sth = obar[0].hid;
    let edh = obar[obar.length - 1].hid;
    console.log("wrote bulk", coll, len, "..", sth, "->", edh);
  } catch (err) {
    console.log("err mongo bulk", coll, coll, obar && obar[0]?.hid);
    console.log(err);
  }
};

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

const get_all_hids = async () => {
  let ed = await get_ed_horse();
  let st = 1;
  let hids = new Array(ed - st + 1).fill(0).map((e, i) => i + st);
  return hids;
};
const get_range_hids = async (st, ed) => {
  let hids = new Array(ed - st + 1).fill(0).map((e, i) => i + st);
  return hids;
};

const prize_id = (pos) => {
  if (pos == 1) return "first";
  if (pos == 2) return "second";
  if (pos == 3) return "third";
  return null;
};

const get_prize = async ({ race_id, hid }) => {
  // console.log("get_prize");
  /*
  select
    rh.horse_id as hid,
    r.id as rid,
    r.start_time as date,
    rh.details->'position' as pos,
    r.details->'prizePool' as prizepool
  from race_horses rh
  JOIN races r on r.id = rh.race_id
  where
   rh.race_id='maM3O6O2'
  and
   rh.horse_id=209973
  limit 1
  */
  // let { race_id, hid } = ob;
  hid = parseInt(hid);
  if (!hid || _.isNaN(hid) || !race_id) return {};
  let result = await knex_conn
    .select(
      `rh.horse_id AS hid`,
      `r.id AS rid`,
      `r.start_time AS date`,
      knex_conn.raw(`rh.details -> 'position' AS position`),
      knex_conn.raw(`r.details -> 'fee' AS fee`),
      knex_conn.raw(`r.details -> 'prizePool' AS prizepool`)
    )
    .from(`race_horses as rh`)
    .innerJoin(`races as r`, function () {
      this.on("r.id", "=", "rh.race_id");
    })
    .where("rh.race_id", "=", race_id)
    .andWhere("rh.horse_id", "=", hid)
    .limit(1);
  if (_.isEmpty(result)) return {};
  result = Array.from(result)[0];
  let prize = result.prizepool[prize_id(result.position)] ?? 0;
  delete result.prizepool;
  result.prize = parseFloat(prize) / 1e18;
  return result;
};

const next_run = (cron_str) => {
  const c_itvl = cron_parser.parseExpression(cron_str);
  return c_itvl.next().toISOString();
};

const print_cron_details = (cron_str) => {
  const c_itvl = cron_parser.parseExpression(cron_str);
  let next = c_itvl.next().toISOString();
  let every = cronstrue.toString(cron_str);
  console.log("next:", next);
  console.log("every:", every);
};

const get_races_n = async (hid) => {
  let bb =
    (await zed_db.db
      .collection("rating_blood3")
      .findOne({ hid }, { projection: { hid: 1, races_n: 1 } })) || {};
  let races_n = bb?.races_n ?? null;
  return races_n;
};
const get_races_n_zed = async (hid) => {
  let bb = await zedf.horse(hid);
  let races_n = bb?.number_of_races ?? null;
  return races_n;
};

const get_parents = async (hid) => {
  hid = parseInt(hid);
  let hdoc = await zed_db.db
    .collection("horse_details")
    .findOne({ hid }, { projection: { parents: 1 } });
  // console.log(hid, hdoc);
  let { parents = null } = hdoc;
  if (_.isEmpty(parents)) return null;
  let { mother, father } = parents;
  if (mother == null || father == null) return null;
  return { mother, father };
};

const get_ymca_avgs = async ({ bloodline, breed_type, genotype }) => {
  let doc_id = "ymca2-global-avgs";
  let id = `${bloodline}-${breed_type}-${genotype}`;
  let doc = await zed_db.db
    .collection("requirements")
    .findOne({ id: doc_id }, { projection: { [`avg_ob.${id}`]: 1 } });
  // console.log(id);
  let this_ob = doc.avg_ob[id] || {};
  return this_ob;
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

const valid_b5 = async (hids) => {
  if (_.isEmpty(hids)) return [];
  let docs =
    (await zed_db.db
      .collection("horse_details")
      .find({ hid: { $in: hids }, tx_date: { $gte: v5_conf.st_date } })
      .toArray()) || [];
  return _.map(docs, "hid");
};

const get_owner_horses_zed_hids = async ({ oid, offset = 0 }) => {
  let api = `https://api.zed.run/api/v1/horses/get_user_horses?public_address=${oid}&offset=${offset}`;
  let data = (await zedf.get(api)) || [];
  if (_.isEmpty(data)) return [];
  let ar = data.map((e) => e.horse_id);
  let afters = await get_owner_horses_zed_hids({ oid, offset: offset + 10 });
  if (!_.isEmpty(afters)) ar = [...ar, ...afters];
  return ar;
};

const get_90d_range = () => {
  let st = moment().add(-90, "days").toISOString();
  let ed = moment().add(0, "days").toISOString();
  return [st, ed];
};

const get_date_range_fromto = (a, b, c, d) => {
  let st = moment().add(a, b).toISOString();
  let ed = moment().add(c, d).toISOString();
  return [st, ed];
};

const z_mi_mx = {
  "Nakamoto-genesis": [1, 2],
  "Nakamoto-legendary": [2, 4],
  "Nakamoto-exclusive": [3, 268],
  "Nakamoto-elite": [4, 268],
  "Nakamoto-cross": [5, 268],
  "Nakamoto-pacer": [7, 268],
  "Szabo-genesis": [3, 4],
  "Szabo-legendary": [4, 8],
  "Szabo-exclusive": [5, 268],
  "Szabo-elite": [6, 268],
  "Szabo-cross": [7, 268],
  "Szabo-pacer": [8, 268],
  "Finney-genesis": [5, 7],
  "Finney-legendary": [6, 14],
  "Finney-exclusive": [7, 268],
  "Finney-elite": [8, 268],
  "Finney-cross": [9, 268],
  "Finney-pacer": [10, 268],
  "Buterin-genesis": [8, 10],
  "Buterin-legendary": [9, 20],
  "Buterin-exclusive": [10, 268],
  "Buterin-elite": [11, 268],
  "Buterin-cross": [12, 268],
  "Buterin-pacer": [13, 268],
};

const filter_r1000 = (races) => {
  if (_.isEmpty(races)) return [];
  races = races.filter((e) => e.thisclass != 1000);
  return races;
};

const ag_look = (coll, loc, frn, as, preserve = true, project = null) => {
  let ag = [
    {
      $lookup: {
        from: coll,
        localField: loc,
        foreignField: frn,
        as: as,
      },
    },
    {
      $unwind: {
        path: `$${as}`,
        includeArrayIndex: "0",
        preserveNullAndEmptyArrays: preserve,
      },
    },
  ];
  if (!_.isEmpty(project)) ag.push({ $project: project });
  return ag;
};

const jparse = (c) => {
  try {
    return JSON.parse(c);
  } catch (err) {
    console.log("jparse err ", err.message);
    return null;
  }
};

const jstr = (c) => {
  return JSON.stringify(c);
};

const filt_valid_hids = async (hids) => {
  let docs = await zed_db.db
    .collection("horse_details")
    .find({ hid: { $in: hids } }, { projection: { hid: 1, _id: 0 } })
    .toArray();
  hids = _.map(docs, "hid");
  return hids;
};

const filt_valid_hids_range = async (a, b) => {
  let docs = await zed_db.db
    .collection("horse_details")
    .find({ hid: { $gte: a, $lte: b } }, { projection: { hid: 1, _id: 0 } })
    .toArray();
  hids = _.map(docs, "hid");
  return hids;
};

const cyclic_depedency = {
  get_races_of_hid,
  from_ch_zed_collection,
  struct_race_row_data,
  progress_bar,
  initiate,
  general_bulk_push,
  get_ed_horse,
  get_all_hids,
  get_prize,
  prize_id,
  jparse,
  next_run,
  get_races_n,
  get_parents,
  get_races_n_zed,
  get_ymca_avgs,
  print_cron_details,
  add_hdocs,
  struct_zed_hdoc,
  get_range_hids,
  valid_b5,
  get_owner_horses_zed_hids,
  get_90d_range,
  get_date_range_fromto,
  z_mi_mx,
  filter_r1000,
  ag_look,
  jparse,
  jstr,
  filt_valid_hids,
  filt_valid_hids_range,
};

module.exports = cyclic_depedency;
