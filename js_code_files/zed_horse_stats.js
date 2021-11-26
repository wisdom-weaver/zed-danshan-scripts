const _ = require("lodash");
const { eth_runner_fn } = require("./base");
const { get_races_of_hid } = require("./cyclic_dependency");
const { init, zed_db } = require("./index-run");
const { get_fee_tag } = require("./utils");
const zedf = require("./zedf");
const { get_ed_horse } = require("./zed_horse_scrape");

const dists = ["all", 1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600];
const places = {
  1: ["1"],
  2: ["2"],
  3: ["3"],
  4: ["4"],
  5: ["5"],
  6: ["6"],
  7: ["7"],
  8: ["8"],
  9: ["9"],
  10: ["10"],
  11: ["11"],
  12: ["12"],
  "1_2": ["1", "2"],
  "11_12": ["11", "12"],
};
const get_roi = (races) => {
  const calc_earning = ({ place, fee }) => {
    if (place == 1) return fee * 7.19;
    if (place == 2) return fee * 2.39;
    if (place == 3) return fee * 1.19;
    return 0;
  };
  let tot_fee = races.reduce((acc, ea) => acc + ea.entryfee_usd, 0);
  let tot_earn = races.reduce(
    (acc, ea) => acc + calc_earning({ place: ea.place, fee: ea.entryfee_usd }),
    0
  );
  let tot_profit = parseFloat(tot_earn - tot_fee);
  let roi = (tot_profit * 100) / (tot_fee || 1);
  return roi;
};

const get_horse_stats = async ({ hid }) => {
  try {
    hid = parseInt(hid);
    let horse = await zedf.horse(hid);
    let name = horse.hash_info.name;
    let genotype = horse.genotype;
    let races = (await get_races_of_hid(hid)) || [];
    races = _.sortBy(races, "date");
    let race_n = races.length;
    let race_first = races[0]?.date || null;
    let race_last = races[races.length - 1]?.date || null;
    let paid_races = _.filter(races, (i) => i.fee_tag !== "F") || [];
    let free_races = _.filter(races, (i) => i.fee_tag === "F") || [];
    let paid_n = paid_races.length;

    let avg_paid_fee_usd = _.chain(paid_races)
      .map("entryfee_usd")
      .mean()
      .value();
    let avg_paid_fee_tag = get_fee_tag(avg_paid_fee_usd);
    let avg_paid_class = _.chain(paid_races).map("thisclass").mean().value();
    avg_paid_class = Math.round(avg_paid_class);

    let free = {
      n: free_races.length,
      per: (free_races.length / race_n) * 100,
    };

    let paid_races_dist = dists.map((dist) => {
      if (dist == "all") return [dist, paid_races];
      let filt = _.filter(paid_races, { distance: dist }) || [];
      return [dist, filt];
    });
    paid_races_dist = _.fromPairs(paid_races_dist);

    for (let dist in paid_races_dist) {
      if (dist !== "all") dist = parseInt(dist);
      let filt = paid_races_dist[dist];
      let n = filt.length;
      let flame_n = _.filter(filt, { flame: 1 })?.length || 0;
      let flame_per = (flame_n / n) * 100;

      let roi = get_roi(filt);

      let places_ob = _.entries(places).map(([k, p_range]) => {
        let p =
          _.filter(filt, (i) => p_range.includes(i.place.toString()))?.length ||
          0;
        let per = (p / n) * 100;
        let p_ob = { p, per };
        return [k, p_ob];
      });
      places_ob = _.fromPairs(places_ob);

      let ob = {
        dist,
        n,
        roi,
        places: places_ob,
        flame_n,
        flame_per,
      };
      paid_races_dist[dist] = ob;
    }

    return {
      hid,
      name,
      genotype,
      race_n,
      paid_n,
      race_first,
      race_last,
      avg_paid_fee_usd,
      avg_paid_fee_tag,
      avg_paid_class,
      free,
      paid: paid_races_dist,
    };
  } catch (err) {
    console.log("err horse_stats", hid);
  }
};
const gen_and_upload_horse_stats = async ({ hid }) => {
  let stats = await get_horse_stats({ hid });
  await zed_db.db
    .collection("horse_stats")
    .updateOne({ hid }, { $set: stats }, { upsert: true });
  console.log("done stats", hid);
};
const horse_stats_range = async (range, cs = 5) => {
  await init();
  await eth_runner_fn();
  for (let chunk of _.chunk(range, cs)) {
    let ar = await Promise.all(chunk.map((hid) => get_horse_stats({ hid })));
    let bulk = [];
    for (let ob of ar) {
      if (_.isEmpty(ob)) continue;
      bulk.push({
        updateOne: {
          filter: { hid: ob.hid },
          update: { $set: ob },
          upsert: true,
        },
      });
    }
    await zed_db.db.collection("horse_stats").bulkWrite(bulk);
    console.log("done", chunk.toString());
  }
};
const horse_stats_all = async (st, ed, cs = 5) => {
  await init();
  if (!st) st = 1;
  if (!ed) ed = await get_ed_horse();
  console.log("horse_stats_all", st, ed);
  let hids = new Array(ed - st + 1).fill(0).map((ea, idx) => st + idx);
  // let hids = [3312, 15147];
  await horse_stats_range(hids, cs);
  console.log("COMPLETED horse_stats_all");
};

module.exports = {
  horse_stats_all,
  gen_and_upload_horse_stats,
};
