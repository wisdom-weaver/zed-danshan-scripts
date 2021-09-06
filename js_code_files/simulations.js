const _ = require("lodash");
const { fetch_r } = require("./utils.js");
const { init, zed_ch, zed_db } = require("./index-run");
const {
  from_ch_zed_collection,
  struct_race_row_data,
  get_races_of_hid,
} = require("./cyclic_dependency");
const { get_adjusted_finish_times } = require("./zed_races_adjusted_finish_time.js");

let global = {
  1000: { mean: 57.72, sd: 0.86 },
  1200: { mean: 71.41, sd: 1.08 },
  1400: { mean: 83.61, sd: 1.28 },
  1600: { mean: 95.77, sd: 1.47 },
  1800: { mean: 107.87, sd: 1.64 },
  2000: { mean: 119.9, sd: 1.87 },
  2200: { mean: 131.97, sd: 2.15 },
  2400: { mean: 143.97, sd: 2.5 },
  2600: { mean: 156.03, sd: 2.93 },
};

const get_hid_doc = async (hid) => {
  let api = `https://api.zed.run/api/v1/horses/get/${hid}`;
  let doc = await fetch_r(api);
  return doc;
};
const get_race_doc = async (rid) => {
  let api = `https://racing-api.zed.run/api/v1/races?race_id=${rid}`;
  let doc = await fetch_r(api);
  if (_.isEmpty(doc) || doc.error) return null;

  let {
    length: dist,
    gates,
    final_positions,
    class: race_class,
    fee,
    name: race_name,
    start_time,
    status,
  } = doc;
  let hids = _.values(gates);

  let names_ob;
  if (_.isEmpty(final_positions)) {
    names_ob = await Promise.all(
      hids.map((h) =>
        get_hid_doc(h).then((d) => [h, d?.hash_info?.name || "-"])
      )
    );
    names_ob = _.fromPairs(names_ob);
  } else
    names_ob = _.chain(final_positions)
      .keyBy("horse_id")
      .mapValues("name")
      .value();

  return {
    dist,
    race_class,
    fee,
    race_name,
    start_time,
    status,
    hids,
    names_ob,
  };
};
const get_all_dist_stats = async (hid) => {
  hid = parseFloat(hid);
  let races = (await get_races_of_hid(hid)) || [];
  // console.table(races);
  let mini = {};
  races.forEach((r) => {
    let { distance, finishtime } = r;
    mini[distance] = [...(mini[distance] || []), finishtime];
  });

  let ds = [1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600];

  let p_ar = [];
  for (let d of ds) {
    let d_ar = mini[d] || [];
    let min_t = _.min(d_ar);
    let max_t = _.max(d_ar);
    let mean_t = _.mean(d_ar);
    let range = max_t - min_t;

    let st_dev = (() => {
      let devs = d_ar.map((e) => e - mean_t);
      devs = devs.map((e) => Math.pow(e, 2));
      return Math.sqrt(_.sum(devs) / devs.length);
    })();

    p_ar.push({
      hid,
      d,
      len: d_ar.length,
      min_t,
      max_t,
      mean_t,
      range,
      st_dev,
    });
  }
  // console.table(pob);
  return p_ar || [];
};
const get_dist_stats = async (hid, d) => {
  hid = parseFloat(hid);
  let races = (await get_races_of_hid(hid)) || [];
  let d_ar = _.chain(races).filter({ distance: d }).value();
  let fin_ar = [];
  // let cs = 2; d_ar = d_ar.slice(0, 5);
  let cs = 25;
  for (let chunk of _.chunk(d_ar, cs)) {
    let chunk_ar = await Promise.all(
      chunk.map((d_el) => {
        let { raceid, hid } = d_el;
        return get_adjusted_finish_times(raceid).then((data) =>
          _.find(data, { hid })
        );
      })
    );
    chunk_ar = _.map(chunk_ar, "adjfinishtime");
    fin_ar = [...fin_ar, ...chunk_ar];
  }

  d_ar = fin_ar;

  let min_t = _.min(d_ar);
  let max_t = _.max(d_ar);
  let mean_t = _.mean(d_ar);
  let range = max_t - min_t;

  let st_dev = (() => {
    let devs = d_ar.map((e) => e - mean_t);
    devs = devs.map((e) => Math.pow(e, 2));
    return Math.sqrt(_.sum(devs) / devs.length);
  })();

  return {
    hid,
    d,
    len: d_ar.length,
    min_t,
    max_t,
    mean_t,
    range,
    st_dev,
  };
};
const get_simulation_base_data = async (rid) => {
  if (!rid) return null;
  let doc = await get_race_doc(rid);
  if (_.isEmpty(doc)) return null;
  let { hids, dist, names_ob, race_class, fee, race_name, start_time, status } =
    doc;
  let stats_ob = await Promise.all(
    hids.map((hid) => get_dist_stats(hid, dist))
  );
  stats_ob = stats_ob.map((a) => ({ ...a, name: names_ob[a.hid] }));
  return {
    rid,
    dist,
    stats_ob,
    race_class,
    fee,
    race_name,
    start_time,
    status,
  };
};

// router.get("/adjfinishtime/:rid", async (req, res) => {
//   let { rid } = req.params;
//   let ob = await get_adjusted_finish_times(rid);
//   res.send(ob);
// });

// router.get("/race/:rid", async (req, res) => {
//   const { rid = null } = req.params || {};
//   const ob = await get_simulation_base_data(rid);
//   if (_.isEmpty(ob)) res.send({ err: "couldn'r calculate horse earnings" });
//   res.send(ob);
// });

// router.get("/test", async (req, res) => {
//   // await get_dist_stats(21112, 1600);
//   return res.send("ok");
// });

// router.get("/", (req, res) => res.send("simulation"));

// let simulation_router = router;

const runner = async () => {
  await init();
  let rid = "5HIArXWg";
  console.log("starting", rid);
  let ob = await get_simulation_base_data(rid);
  console.log(ob);
};
// runner();
