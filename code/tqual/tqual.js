const _ = require("lodash");
const { zed_db, zed_ch } = require("../connection/mongo_connect");
const { nano, iso, getv } = require("../utils/utils");
const moment = require("moment");
const red = require("../connection/redis");
const {
  struct_race_row_data,
  jstr,
  jparse,
} = require("../utils/cyclic_dependency");

const tcoll = "tquall_master";
let test_mode = true;

const get_tinfo = async (tid, projection = null) => {
  let q = { tid };
  let parr = _.isNil(projection) ? [q] : [q, projection];
  let doc = await zed_db.db.collection(tcoll).findOne(...parr);
  return doc;
};

const traces_getter = async (rar) => {
  let fins = await Promise.all(
    rar.map(async (e) => {
      let redid = `tqual_rar:${e.raceid}:${e.hid}`;
      let cache = await red.rget(redid);
      let cache_exists = _.isEmpty(cache) ? false : true;
      return [e.raceid, e.hid, redid, cache_exists, cache];
    })
  );

  console.table(fins);

  let cache_races = _.chain(fins)
    .filter((e) => e[3] == true)
    .map(4)
    .compact()
    .value();
  console.log("cache", cache_races.length);

  let rids_not = _.map(
    _.filter(fins, (e) => e[3] == false),
    0
  );
  console.log("redids_not", rids_not.length);
  rids_not = _.uniq(rids_not);
  console.log("rids_not", rids_not.length);

  let races = [];

  races = [...races, ...cache_races];

  let i = 0;
  for (let chu of _.chunk(rids_not, 20)) {
    let earaces = await zed_ch.db
      .collection("zed")
      .find({ 4: { $in: chu } })
      .toArray();
    earaces = struct_race_row_data(earaces);
    for (let r of earaces) {
      let redid = `tqual_rar:${r.raceid}:${r.hid}`;
      await red.rset(redid, r, 60 * 60);
    }
    races.push(earaces);
    i += chu.length;
    console.log("got", i, jstr(chu));
  }
  races = _.flatten(races);
  console.log("ALL", races.length);
  console.table(races.slice(0, 10));

  return races;
};

const calc_t_score = (rrow, tdoc) => {
  // if (test_mode) console.log(rrow);
  let tot = 0;
  for (let [sidx, e] of _.entries(tdoc.score_cr)) {
    // e of score_cr[] {
    //   thisclass: [],
    //   distance: [],
    //   fee_tag: [],
    //   flame: [ 0 ],
    //   pos: [ 3 ],
    //   score: 2
    // }
    if (_.isEmpty(e.thisclass));
    else if (!e.thisclass.includes(rrow.thisclass)) {
      if (test_mode)
        console.log(rrow.rid, "exit rc", rrow.thisclass, "n", e.thisclass);
      continue;
    }
    if (_.isEmpty(e.distance));
    else if (!e.distance.includes(rrow.distance)) {
      if (test_mode)
        console.log(rrow.rid, "exit distance", rrow.distance, "n", e.distance);
      continue;
    }
    if (_.isEmpty(e.fee_tag));
    else if (!e.fee_tag.includes(rrow.fee_tag)) {
      if (test_mode)
        console.log(rrow.rid, "exit fee_tag", rrow.fee_tag, "n", e.fee_tag);
      continue;
    }
    if (_.isEmpty(e.flame));
    else if (!e.flame.includes(rrow.flame)) {
      if (test_mode)
        console.log(rrow.rid, "exit flame", rrow.flame, "n", e.flame);
      continue;
    }
    if (_.isEmpty(e.pos));
    else if (!e.pos.includes(rrow.place)) {
      if (test_mode)
        console.log(rrow.rid, "exit place", rrow.place, "n", e.pos);
      continue;
    }
    if (test_mode) console.log(rrow.rid, `(${sidx}) conforms`, e.score);
    tot += e.score || 0;
  }
  return tot;
};

const normal_races_do = async (hid, tdoc, races) => {
  console.log(hid);
  console.log(tdoc.score_cr);
  races = races.map((rrow) => {
    console.table([rrow]);
    let score = calc_t_score(rrow, tdoc);
    console.log(rrow.raceid, { score });
    rrow.score = score;
    return rrow;
  });

  races = _.sortBy(races, (r) => -nano(r.date));

  let traces_n = races.length;
  let tot_score = _.sumBy(races, "score");
  if ([NaN, undefined, null].includes(tot_score)) tot_score = 0;
  let avg_score = (tot_score || 0) / (traces_n || 1);

  let races_map = _.clone(races).map((e) => {
    return { raceid: e.raceid, score: e.score };
  });

  update_doc = {
    hid,
    traces_n,
    tot_score,
    avg_score,
    races_map,
  };
  return update_doc;
};

const eval_leaderboard = async ({ tdoc, races }) => {
  let group = _.groupBy(races, "hid");
  let hdocs = {};
  for (let [hid, rs] of _.entries(group)) {
    hid = parseInt(hid);
    console.log("HORSE", hid);
    hdocs[hid] = await normal_races_do(hid, tdoc, rs);
    console.log(hdocs[hid]);
  }

  let mode = tdoc.score_mode;
  let type = tdoc.type;
  let k =
    (mode == "total" && "tot_score") || (mode == "avg" && "avg_score") || null;
  let lim = 5;
  console.log({ type, mode, lim });

  if (!k) return hdocs;
  hdocs = _.sortBy(hdocs, (i) => {
    let val = Number(i[k]);
    let n = Number(i["traces_n"]);
    if (!n) return 1e14;
    return [NaN, undefined, 0, null].includes(val) ? -n : -(val * 1000 + n);
  });
  let i = 0;
  hdocs = _.map(hdocs, (e) => {
    let rank = null;
    if (e[k] != 0 && e.traces_n >= lim) rank = ++i;
    return { ...e, rank };
  });
  hdocs = _.sortBy(hdocs, (e) => {
    if (_.isNil(e.rank)) return 1e14;
    return _.toNumber(e.rank);
  });

  return hdocs;
};

const run_tid = async ({ tid }) => {
  console.log("running tid", tid);
  let tdoc = await get_tinfo(tid);

  let [st, ed] = [tdoc.tourney_st, tdoc.tourney_ed];
  console.log("Getting", st, "->>", ed);

  const rc_cr = [];
  let tclass_type = getv(tdoc, `race_cr.tclasstype`);
  if (tclass_type.includes("open")) rc_cr.push(1000);
  if (tclass_type.includes("free")) rc_cr.push(0);

  let rar = [];
  for (let now = nano(st); now < nano(ed); ) {
    let nst = iso(now);
    let ned = moment(nst).add(10, "minutes").toISOString();
    if (ned > ed) ned = ed;
    let redid = `tqual_rids:${tid}:${nst}->${ned}`;
    let ea;
    ea = await red.rget(redid);
    if (ea && !_.inRange(Date.now(), nano(nst), nano(ned))) {
      console.log([nst, ned], "cache:: ", ea.length);
    } else {
      ea = await zed_ch.db
        .collection("zed")
        .find(
          {
            2: { $gte: nst, $lte: ned },
            5: { $in: rc_cr },
          },
          { projection: { 4: 1, 6: 1, _id: 0 } }
        )
        .toArray();

      ea = ea?.map((e) => {
        return {
          raceid: e[4],
          hid: e[6],
        };
      });
      console.log([nst, ned], "got:: ", ea.length);
      await red.rset(redid, ea, 60 * 60);
    }
    rar.push(ea);
    now = nano(ned) + 1;
  }
  rar = _.flatten(rar);
  console.log("got TOTAL:", rar.length);

  let races = await traces_getter(rar);
  console.log("got all races");

  const leaderboard = await eval_leaderboard({ tdoc, races });
  console.table(leaderboard);
};

const test = async () => {
  console.log("test");
  let tid = "87f37293";
  await run_tid({ tid });
};

const main_runner = async () => {
  console.log("tqual");
  let [_node, _cfile, arg1, arg2, arg3, arg4, arg5] = process.argv;
  if (arg2 == "test") await test();
};
const tqual = {
  main_runner,
};
module.exports = { tqual };
