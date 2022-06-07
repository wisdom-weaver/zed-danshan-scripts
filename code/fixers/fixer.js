const _ = require("lodash");
const { zed_db, zed_ch } = require("../connection/mongo_connect");
const { get_ed_horse } = require("../utils/cyclic_dependency");
const { getv, cdelay } = require("../utils/utils");
const mega = require("../v3/mega");

const race0err = async () => {
  let cs = 200;
  let ed = await get_ed_horse();
  for (let i = 0; i <= ed; i += cs) {
    let exec = [];
    let [s, e] = [i, Math.min(i + cs, ed)];
    let db_n = await zed_db.db
      .collection("horse_details")
      .aggregate([
        {
          $match: {
            hid: {
              $gte: s,
              $lte: e,
            },
          },
        },
        {
          $lookup: {
            from: "rating_blood3",
            localField: "hid",
            foreignField: "hid",
            as: "doc_blood3",
          },
        },
        {
          $unwind: {
            path: "$doc_blood3",
            includeArrayIndex: "0",
            preserveNullAndEmptyArrays: false,
          },
        },
        {
          $project: {
            hid: 1,
            races_n: "$doc_blood3.races_n",
          },
        },
      ])
      .toArray();
    db_n = _.keyBy(db_n, "hid");
    // console.table(db_n);

    let ch_n = await zed_ch.db
      .collection("zed")
      .aggregate([
        {
          $match: {
            6: {
              $gte: s,
              $lte: e,
            },
          },
        },
        {
          $group: {
            _id: "$6",
            races_rn: {
              $sum: 1,
            },
          },
        },
      ])
      .toArray();
    ch_n = _.keyBy(ch_n, "_id");
    // console.table(ch_n);

    for (let j = s; j <= e; j++) {
      let r1 = getv(db_n, `${j}.races_n`) ?? 0;
      let r2 = getv(ch_n, `${j}.races_rn`) ?? 0;
      if (Math.abs(r1 - r5) > 7) exec.push({ hid: j, r1, r2 });
    }
    console.log(`${s}->${e} :: ${exec.length} found`);
    await mega.only(_.map(exec, "hid"));
    // console.table(exec);
    cdelay(2000);
  }
};

const main_runner = async () => {
  let args = process.argv;
  let [n, f, arg1, arg2, arg3, arg4] = args;
  if (arg2 == "race0err") await race0err();
};

const fixers = { main_runner };
module.exports = { fixers };
1;
