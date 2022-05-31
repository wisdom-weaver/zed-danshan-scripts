const {
  get_races_of_hid,
  get_ed_horse,
  get_90d_range,
} = require("../utils/cyclic_dependency");
const mdb = require("../connection/mongo_connect");
const _ = require("lodash");
const bulk = require("../utils/bulk");
const { zed_db, zed_ch } = require("../connection/mongo_connect");
const { calc_race_score } = require("./race_score");
const { push_bulk } = require("../utils/bulk");
const cyclic_depedency = require("../utils/cyclic_dependency");
const coll = "rating_blood3";
const name = "rating_blood v3";
const cs = 200;
const test_mode = 0;

const calc_roi = (r) => {
  let fee = _.reduce(r, (n, i) => n + (i?.entryfee_usd || 0), 0);
  let profit =
    _.sum(
      [
        [1, 7.19],
        [2, 2.39],
        [3, 1.19],
      ].map(([p, ratio]) => {
        let prof_p = r.map((i) => {
          let { place, entryfee_usd } = i;
          if (place == p) return entryfee_usd * ratio;
          else return 0;
        });
        return _.sum(prof_p);
      })
    ) ?? 0;
  return ((profit - fee) / (fee || 1)) * 100;
};
const calc_win_rate = (r) => {
  let win_rate = _.filter(r, (i) => ["1"].includes(i.place.toString()));
  win_rate = ((win_rate?.length || 0) / (r?.length || 1)) * 100;
  return win_rate;
};
const calc_wins = (r) => {
  let win_rate = _.filter(r, (i) => ["1"].includes(i.place.toString()));
  return win_rate?.length || 0;
};
const calc_flame_rate = (r) => {
  let flame_per = _.filter(r, (i) => i.flame == 1);
  flame_per = ((flame_per?.length || 0) / (r?.length || 1)) * 100;
  return flame_per;
};
const calc_rat = (races = []) => {
  let r_ob = races.map((r) => {
    let { thisclass: rc, fee_tag, place: position, flame } = r;
    let score = calc_race_score({ rc, fee_tag, position, flame });
    let final_score = score * 0.1;
    return {
      rc,
      fee_tag,
      position,
      flame,
      score,
      final_score,
    };
  });
  let rat = _.meanBy(r_ob, "final_score") ?? null;
  if (_.isNaN(rat)) return null;
  return rat;
};
const calc_profit = (races) => {
  let earn = 0;
  let fee = 0;
  for (let race of races) {
    let { entryfee = 0, place } = race;
    entryfee = parseFloat(entryfee);
    let this_earn = 0;
    if (parseInt(place) == 1) this_earn = entryfee * 12 * 0.6;
    if (parseInt(place) == 2) this_earn = entryfee * 12 * 0.25;
    if (parseInt(place) == 3) this_earn = entryfee * 12 * 0.15;
    earn += this_earn;
    fee += entryfee;
  }
  const profit = earn - fee;
  // console.log({ earn, fee, profit });
  return profit;
};
const calc_pts = (p) => {
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
const calc_avg_pts = (races) => {
  let all_pts = races.map((r) => {
    return calc_pts(r.place);
  });
  let tot_pts = _.sum(all_pts) ?? 0;
  return (tot_pts ?? 0) / (all_pts.length || 1);
};

const def_overall = {
  rat: null,
  profit: null,
  win_rate: null,
  flame_rate: null,
  type: "overall",
};
const calc_overall_rat = async ({ hid, races = [], tc }) => {
  hid = parseInt(hid);
  if (races?.length == 0) return { ...def_overall, rated_type: "NR" };
  let filt_races = races;
  let r_ob = filt_races.map((r) => {
    let { thisclass: rc, fee_tag, place: position, flame } = r;
    let score = calc_race_score({ rc, fee_tag, position, flame });
    let final_score = score * 0.1;
    return {
      rc,
      fee_tag,
      position,
      flame,
      score,
      final_score,
    };
  });
  let rat = _.meanBy(r_ob, "final_score") ?? null;
  let wins = calc_wins(filt_races);
  let win_rate = calc_win_rate(filt_races);
  let profit = calc_profit(filt_races);
  let flame_rate = calc_flame_rate(filt_races);
  if (flame_rate >= 50 || wins >= 2)
    return { rat, profit, win_rate, flame_rate, type: "all", rated_type: "GH" };
  else return { ...def_overall, rated_type: "CH" };
};
const def_tunnel_rat = {
  c: null,
  f: null,
  t: null,
  rat: null,
  win_rate: null,
  profit: null,
  avg_pts: null,
  type: "tunnel",
};
const calc_tunnel_rat = async ({ hid, races = [] }) => {
  if (races.length == 0) return { ...def_tunnel_rat, rated_type: "NR" };
  let ar = [];
  let max_ob;
  c_loop: for (let c of [1, 2, 3, 4, 5]) {
    f_loop: for (let f of ["A", "B", "C", "D", "E"]) {
      t_loop: for (let t of ["S", "M", "D"]) {
        let query = {};
        if (c !== "#") query.thisclass = c;
        if (t !== "#") query.tunnel = t;
        if (f !== "#") query.fee_tag = f;
        let filt = _.filter(races, query);
        let wins = calc_wins(filt);
        let flame_rate = calc_flame_rate(filt);
        if (flame_rate >= 50 || wins >= 2) {
          let ea_ob = { c, f, t, flame_rate, wins };
          // console.log(ea_ob);
          ar.push(ea_ob);
        }
      }
      if (ar.length > 0) {
        max_ob = _.maxBy(ar, (i) => +i.wins);
        break c_loop;
      }
    }
  }
  if (!max_ob) {
    let tun_ob = ["S", "M", "D"].map((tunnel) => {
      let filt_races = _.filter(races, { tunnel });
      let rat = calc_rat(filt_races) ?? null;
      return { rat, t: tunnel };
    });
    max_ob = _.maxBy(tun_ob, "rat");
  }
  // return;
  let tunn_races = _.filter(races, { tunnel: max_ob.t });
  let win_rate = calc_win_rate(tunn_races);
  let profit = calc_profit(tunn_races);
  let avg_pts = calc_avg_pts(tunn_races);
  let rat = calc_rat(tunn_races);
  return {
    hid,
    c: max_ob?.c || null,
    f: max_ob?.f || null,
    t: max_ob?.t || null,
    rat,
    win_rate,
    profit,
    avg_pts,
    rated_type: max_ob?.c ? "GH" : "CH",
    type: "tunnel",
  };
};
const calc = async ({ hid, races = [], tc }) => {
  try {
    hid = parseInt(hid);
    let ob = { hid };
    ob.overall_rat = await calc_overall_rat({ hid, races });
    ob.tunnel_rat = await calc_tunnel_rat({ hid, races });
    ob.races_n = races?.length ?? 0;
    // console.log(ob)
    return ob;
  } catch (err) {
    console.log("err on rating", hid);
    console.log(err);
  }
};
const generate = async (hid) => {
  hid = parseInt(hid);
  // let races = await get_races_of_hid(hid);
  let [st, ed] = cyclic_depedency.get_90d_range();
  let races = await zed_ch.db
    .collection("zed")
    .find({ 2: { $gte: st, $lte: ed }, 6: hid },  )
    .toArray();
  // console.table(races);
  races = cyclic_depedency.struct_race_row_data(races);
  let doc = await zed_db.db
    .collection("horse_details")
    .findOne({ hid }, { tc: 1 });
  let tc = doc?.tc || undefined;
  let ob = await calc({ races, tc, hid });
  if (test_mode) console.log(hid, ob);
  return ob;
};

const generate_ranks = async () => {
  console.log("starting generate_ranks");
  let st = 0;
  let ed = await get_ed_horse();
  let hids = get_hids(st, ed);
  let ar = [];
  for (let chunk of _.chunk(hids, cs)) {
    let chunk_ar = await zed_db.db
      .collection("rating_blood3")
      .find(
        {
          hid: { $in: chunk },
          "tunnel_rat.rated_type": "GH",
        },
        {
          hid: 1,
          "tunnel_rat.rat": 1,
          "tunnel_rat.rated_type": 1,
        }
      )
      .toArray();
    chunk_ar = _.compact(chunk_ar);
    chunk_ar = chunk_ar.map((e) => {
      let { rat, rated_type } = e.tunnel_rat || {};
      return { hid: e.hid, rat, rated_type };
    });
    ar = [...ar, ...chunk_ar];
    let [a, b] = [chunk[0], chunk[chunk.length - 1]];
    console.log("downloaded", a, "to", b);
  }
  console.log("total", ar.length);
  await zed_db.db
    .collection("rating_blood3")
    .updateMany({}, { $set: { rank: null } });
  ar = _.filter(ar, (ea) => ea.rated_type == "GH");
  ar = _.sortBy(ar, (i) => -i.rat);
  console.log("to push", ar.length);
  ar = ar.map((e, i) => {
    return { hid: e.hid, rank: i + 1, rat: e.rat };
  });
  console.table(ar.slice(0, 100));
  for (let chunk of _.chunk(ar, cs)) {
    chunk = chunk.map((ea) => {
      let { hid, rank } = ea;
      return { hid, rank };
    });
    await push_bulk("rating_blood3", chunk);
  }
  console.log("ended generate_ranks");
};

const all = async () => bulk.run_bulk_all(name, generate, coll, cs, test_mode);
const only = async (hids) =>
  bulk.run_bulk_only(name, generate, coll, hids, cs, test_mode);
const range = async (st, ed) =>
  bulk.run_bulk_range(name, generate, coll, st, ed, cs, test_mode);

const test = async (hid) => {
  hid = parseInt(hid);
  // let hid = 126065;
  let races = await get_races_of_hid(hid);
  let ob = await calc_overall_rat({ hid, races });
  console.table(ob);
  let ob3 = await calc_tunnel_rat({ hid, races });
  console.table(ob3);
};
const fix = async () => {
  let hids = await zed_db.db
    .collection("rating_blood3")
    .find({
      $or: [{ races_n: { $exists: false } }, { races_n: { $eq: 0 } }],
    })
    .toArray();
  hids = _.map(hids, "hid");
  console.log("hids.length", hids.length);
  for (let chu of _.chunk(hids, 1000)) {
    if (!_.isEmpty(chu)) {
      // await zed_db.db.collection(coll).deleteMany({ hid: { $in: hids } });
      await only(hids);
    }
  }
};

const rating_blood = {
  test,
  calc,
  generate,
  all,
  only,
  range,
  generate_ranks,
  fix,
};
module.exports = rating_blood;
