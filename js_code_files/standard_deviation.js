const _ = require("lodash");
const { get_races_of_hid } = require("./cyclic_dependency");
const { init, zed_ch } = require("./index-run");
const { write_to_path } = require("./utils");
const app_root = require("app-root-path");

/*
ref: 
https://knowyourhorses.com/horses/16137/speed_statistics
*/

const all_horse_out_path = `${app_root}/data/races_comb/00_all_horses.json`;
const horse_out_path = (hid) =>
  `${app_root}/data/races_comb/horses_${hid}.json`;

const runner_for_each_horse = async (hid) => {
  hid = parseFloat(hid);
  let races = (await get_races_of_hid(hid)) || [];
  // console.table(races);
  let mini = {};
  races.forEach((r) => {
    let { distance, finishtime } = r;
    mini[distance] = [...(mini[distance] || []), finishtime];
  });

  let ds = [1000,1200,1400,1600,1800,2000,2200,2400,2600]

  let p_ar = [];
  _.entries(mini).map(([d, d_ar]) => {
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
  });
  // console.table(pob);

  write_to_path({ file_path: horse_out_path(hid), data: p_ar });
  return p_ar || [];
};
const runner_for_all_horse = async () => {
  await init();
  let st = 1;
  let ed = 94940;
  let cs = 50;
  let ar = [];
  let hids = new Array(ed - st + 1).fill(0).map((e, i) => i + st);
  // let hids = [3312, 15147];
  for (let chunk of _.chunk(hids, cs)) {
    let cs_ar = await Promise.all(
      chunk.map((hid) => runner_for_each_horse(hid))
    );
    cs_ar = _.flatten(cs_ar);
    console.log(chunk[0], " -> ", chunk[chunk.length - 1]);
    ar = [...ar, ...cs_ar];
  }
  console.log("##completed");
  write_to_path({ file_path: all_horse_out_path, data: ar });
};
runner_for_all_horse();
