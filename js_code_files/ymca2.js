const { init, zed_db, zed_ch } = require("./index-run");
const {
  struct_race_row_data,
  general_bulk_push,
  get_ed_horse,
} = require("./cyclic_dependency");
const { calc_median, delay } = require("./utils");
const _ = require("lodash");

const chunk_delay = 200;

const c_tab = {
  0: 1,
  1: 7,
  2: 4.5,
  3: 3,
  4: 2,
  5: 1.5,
};

const f_tab = {
  A: 10,
  B: 10,
  C: 8,
  D: 5,
  E: 2.5,
  F: 1,
};

let pos_tab = {
  1: [7, 10],
  2: [5, 8],
  3: [4, 7],
  4: [2, 4],
  5: [1, 3],
  6: [1, 3],
  7: [1, 3],
  8: [1, 3],
  9: [2, 5],
  10: [3, 6],
  11: [4, 7],
  12: [5, 8],
};

const calc_score = ({ rc, fee_tag, position, flame }) => {
  let c_sc = c_tab[rc] || 0;
  let f_sc = f_tab[fee_tag] || 0;
  let p_sc = pos_tab[position][flame] || 0;
  return c_sc + f_sc + p_sc;
};

const generate_ymca2 = async (hid, print = 0) => {
  try {
    hid = parseInt(hid);
    // let { tc = null } = await zed_db.db
    //   .collection("horse_details")
    //   .findOne({ hid }, { tc: 1 });
    // if (print) console.log(hid, `:`, tc);

    let races = await zed_ch.db
      .collection("zed")
      .find({ 6: hid })
      .sort({ 2: 1 })
      .limit(8)
      .toArray();
    races = struct_race_row_data(races);
    // if (print) console.log(races[0]);

    let r_ob = races.map((r) => {
      let { thisclass: rc, fee_tag, place: position, flame } = r;
      let score = calc_score({ rc, fee_tag, position, flame });
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
    // if (print) console.table(r_ob);

    let ymca2 = calc_median(_.map(r_ob, "final_score")) ?? null;
    if (print) console.log(hid, "ymca2", ymca2);

    return ymca2;
  } catch (err) {
    console.log("err in ymca2", hid, err);
    return null;
  }
};

const ymca2_generator_all_horses = async (cs = 500) => {
  try {
    console.log("ymca2_generator_all_horses");
    await init();
    let st = 1;
    let ed = await get_ed_horse();
    let hids = new Array(ed - st + 1).fill(0).map((ea, idx) => st + idx);
    // hids = [3722];

    console.log("=> STARTED ymca2_generator: ", `${st}:${ed}`);
    for (let chunk of _.chunk(hids, cs)) {
      let [a, b] = [chunk[0], chunk[chunk.length - 1]];
      console.log("\n=> fetching together:", a, "to", b);
      let obar = await Promise.all(
        chunk.map((hid) =>
          generate_ymca2(hid).then((ymca2) => {
            return { hid, ymca2 };
          })
        )
      );
      // console.table(obar);
      try {
        await general_bulk_push("rating_breed2", obar);
      } catch (err) {
        console.log("mongo err", err);
      }
      console.log("! got", chunk[0], " -> ", chunk[chunk.length - 1]);
      await delay(chunk_delay);
    }

    console.log("ended");
  } catch (err) {
    console.log("ERROR fetch_all_horses\n", err);
  }
};

const runner = async () => {
  await init();
  let hid = 132134;
  let ymca2 = await generate_ymca2(hid, 1);
  console.log(ymca2);
};
// runner();

// ymca2_generator_all_horses();

const ymca2 = {
  generate_ymca2,
  ymca2_generator_all_horses,
};
module.exports = ymca2;
