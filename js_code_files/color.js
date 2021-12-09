const { init, zed_db } = require("./index-run");
const _ = require("lodash");
const { add_new_horse_from_zed_in_bulk } = require("./zed_horse_scrape");
const cmap = require("../required_jsons/cmap.json");
const { iso, per } = require("./utils");
const cron = require("node-cron");
const cron_parser = require("cron-parser");

const get_color_pair_data = async (fc, mc) => {
  let dads = await zed_db.db
    .collection("horse_details")
    .find(
      {
        color: fc,
        // breed_type: { $ne: "genesis" },
        horse_type: { $in: ["Colt", "Stallion"] },
      },
      { projection: { hid: 1 } }
    )
    .toArray();
  // console.log(dads);
  dads = _.map(dads, "hid");
  console.log("dad color", fc);
  console.log("dads", dads.length);

  let moms = await zed_db.db
    .collection("horse_details")
    .find(
      {
        color: mc,
        // breed_type: { $ne: "genesis" },
        horse_type: { $in: ["Mare", "Filly"] },
      },
      { projection: { hid: 1 } }
    )
    .toArray();
  moms = _.map(moms, "hid");
  // console.log(moms);
  console.log("mom color", mc);
  console.log("moms", moms.length);

  let offsprings = await zed_db.db
    .collection("horse_details")
    .find(
      {
        "parents.father": { $in: dads },
        "parents.mother": { $in: moms },
      },
      { projection: { hid: 1, color: 1, _id: 0 } }
    )
    .toArray();
  // console.log(offsprings.slice(0, 2));
  console.log("offsprings", offsprings.length);
  let N = offsprings.length;

  let colors = _.chain(offsprings)
    .groupBy("color")
    .entries()
    .map(([c, ar]) => {
      let n = ar.length;
      let prob = n / N;
      let hex = _.find(cmap, { color: c })?.hex;
      // console.log(c, _.map(ar.slice(0, 5), "hid"));
      return { color: c, n, prob, hex };
    })
    .sortBy((i) => -i.prob)
    // .keyBy("color")
    .value();
  console.table(colors);
  // colors_ob = _.keyBy(colors_ob, "color");
  return { N, fc, mc, colors, fc, mc };
};

const fix_colors = async () => {
  // let rs = ["Rare", "Super Rare", "Common"];
  let rs = ["Agressive Azure", "Gainsboro", , "Slate Gray"];
  for (let r of rs) {
    let ar = await zed_db.db
      .collection("horse_details")
      .find({ color: { $regex: r } }, { projection: { hid: 1, _id: 1 } })
      .toArray();
    ar = _.map(ar, "hid");
    console.log(r, ar.length);
    for (let chunk_hids of _.chunk(ar, 5))
      await add_new_horse_from_zed_in_bulk(chunk_hids, 5);
  }
};

const get_parents_color_pair_chart = async () => {
  await init();
  console.log("## get_parents_color_pair_chart");
  console.log("#started", iso());
  let colors = _.map(cmap, "color");
  let ar = [];
  for (let a of colors) for (let b of colors) ar.push({ fc: a, mc: b });
  console.log("tot combination", ar.length);

  let idx = 0;
  let N = ar.length;
  for (let ea of ar) {
    idx++;
    let { fc, mc } = ea;
    console.log(`comb ${per(idx, N)} :: ${idx} of ${N}`, fc, mc);
    let doc = await get_color_pair_data(fc, mc);
    await zed_db.db
      .collection("cmap_combs")
      .updateOne({ fc, mc }, { $set: doc }, { upsert: true });
  }
  console.log("#completed", iso());
};

const get_parents_color_pair_chart_cron = () => {
  let cron_str = "0 0  0 */3 * *";
  console.log(
    "#starting get_parents_color_pair_chart_cron",
    cron_str,
    cron.validate(cron_str)
  );
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("next", c_itvl.next().toISOString());
  const runner = () => {
    console.log("#running get_parents_color_pair_chart_cron");
    console.log("Now run:", new Date().toISOString());
    console.log("Next run:", c_itvl.next().toISOString());
    get_parents_color_pair_chart();
  };
  cron.schedule(cron_str, runner);
};

const upload_cmap = async () => {
  await init();
  for (let row of cmap) {
    console.log(row.color);
    await zed_db.db
      .collection("cmap")
      .updateOne({ color: row.color }, { $set: row }, { upsert: true });
  }
  console.log("completed");
};

const runner = async () => {
  await init();
  console.log("color");
  let fc = "Chartreuse";
  let mc = "Chartreuse";
  // let ob = await get_color_pair_data(fc, mc);
  // console.log(ob);
  // await fix_colors();
  // console.log(cmap.length);

  // await get_parents_color_pair_chart();

  // await upload_cmap()

  // await fix_colors();
};
// runner();

module.exports = {
  get_parents_color_pair_chart,
  get_parents_color_pair_chart_cron,
};
