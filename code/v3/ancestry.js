const _ = require("lodash");
const { zed_db } = require("../connection/mongo_connect");
const bulk = require("../utils/bulk");
const { dec } = require("../utils/utils");

const name = "ancestry";
const coll = "horse_details";
let cs = 1;
let test_mode = 0;

const get_ans_ob = async (hid) => {
  let { bloodline, ancestry } = await zed_db.db
    .collection("horse_details")
    .findOne({ hid }, { projection: { bloodline: 1, ancestry: 1 } });
  return ancestry;
};

const calc = async ({ hid, hdoc }) => {
  if (_.isEmpty(hdoc)) return;
  let { parents, bloodline } = hdoc;
  let { mother, father } = parents;
  if (!mother && !father) {
    return {
      hid,
      ancestry: {
        n: bloodline == "Nakamoto" ? 1 : 0,
        s: bloodline == "Szabo" ? 1 : 0,
        f: bloodline == "Finney" ? 1 : 0,
        b: bloodline == "Buterin" ? 1 : 0,
      },
    };
  }
  let m_ob = await get_ans_ob(mother);
  let f_ob = await get_ans_ob(father);
  if (test_mode) console.log({ m_ob, f_ob });
  let ancestry = {
    n: _.mean([m_ob.n, f_ob.n]),
    s: _.mean([m_ob.s, f_ob.s]),
    f: _.mean([m_ob.f, f_ob.f]),
    b: _.mean([m_ob.b, f_ob.b]),
  };
  return { hid, ancestry };
};
const generate = async (hid) => {
  hid = parseInt(hid);
  let hdoc = await zed_db.db
    .collection("horse_details")
    .findOne({ hid }, { projection: { parents: 1, bloodline: 1, name: 1 } });
  let ob = await calc({ hid, hdoc });
  if (test_mode) console.log(ob);
  return ob;
};

const all = async () => bulk.run_bulk_all(name, generate, coll, cs, test_mode);
const only = async (hids) =>
  bulk.run_bulk_only(name, generate, coll, hids, cs, test_mode);
const range = async (st, ed) =>
  bulk.run_bulk_range(name, generate, coll, st, ed, cs, test_mode);

const test = async (hids) => {
  test_mode = 0;
  for (let hid of hids) {
    await only([hid]);
  }
};

const ancestry = { calc, generate, all, only, range, test };
module.exports = ancestry;
