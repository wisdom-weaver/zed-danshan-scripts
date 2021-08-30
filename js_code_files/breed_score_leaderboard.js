const _ = require("lodash");
const { init, zed_db } = require("./index-run");
const { get_max_horse } = require("./max_horses");
const { delay } = require("./utils");

const write_horse_details_to_hid_br = async (hid) => {
  hid = parseInt(hid);
  if (!hid) return;
  const blood_doc = await zed_db.collection("rating_blood").findOne({ hid });
  if (_.isEmpty(blood_doc) || _.isEmpty(blood_doc?.details)) return;
  const { bloodline, breed_type, genotype, color, horse_type } =
    blood_doc?.details;
  let update_ob = {
    bloodline,
    breed_type,
    genotype,
    color,
    horse_type,
  };
  await zed_db
    .collection("kids")
    .updateOne({ hid }, { $set: update_ob }, { upsert: true });
};

const print_hid_br_doc = async (hid) => {
  let doc = await zed_db.collection("kids").findOne({ hid });
  console.log(doc);
};

const write_horse_details_to_all_br = async () => {
  await init();
  let mx = await get_max_horse();
  let st = 82275;
  let ed = mx;
  console.log("started write_horse_details_to_all_br\n", st, ":", ed);
  let cs = 25;
  let hids = new Array(ed - st + 1).fill(0).map((e, i) => st + i);
  // hids = hids.slice(0, 10); // for testing
  for (let chunk of _.chunk(hids, cs)) {
    await Promise.all(chunk.map((hid) => write_horse_details_to_hid_br(hid)));
    console.log(chunk[0], "->", chunk[chunk.length - 1]);
  }
  console.log("##completed");
};
const get_filtered_br = async () => {
  await init();
  let filter_ob = {
    breed_type: ["genesis"],
    bloodline: ["Nakamoto"],
    horse_type: ["Stallion"],
    genotype: ["Z1"],
  };
  let docs = await zed_db
    .collection("kids")
    .find(
      {
        ...(_.isEmpty(filter_ob?.bloodline)
          ? {}
          : { bloodline: { $in: filter_ob.bloodline } }),
        ...(_.isEmpty(filter_ob?.breed_type)
          ? {}
          : { breed_type: { $in: filter_ob.breed_type } }),
        ...(_.isEmpty(filter_ob?.genotype)
          ? {}
          : { genotype: { $in: filter_ob.genotype } }),
        ...(_.isEmpty(filter_ob?.color)
          ? {}
          : { color: { $in: filter_ob.color } }),
        ...(_.isEmpty(filter_ob?.horse_type)
          ? {}
          : { horse_type: { $in: filter_ob.horse_type } }),
        gz_med: { $ne: null },
      },
      { sort: { gz_med: 1 }, projection: { hid: 1, _id: 0, gz_med: 1 } }
    )
    .limit(25)
    .toArray();
  console.log(docs);
};
get_filtered_br();

module.exports = {
  write_horse_details_to_all_br,
  write_horse_details_to_hid_br,
};
