const _ = require("lodash");
const { zed_db } = require("../connection/mongo_connect");
const { getv } = require("../utils/utils");

async function get_parents(hid) {
  if (!hid) return null;
  hid = parseInt(hid);
  let doc =
    (await zed_db.db.collection("horse_details").findOne({ hid })) || {};
  let parents = getv(doc, "parents");
  if (_.isEmpty(_.compact(_.values(parents)))) return null;
  return parents;
}
async function get_ancesters_stub({
  refhid,
  father_id,
  mother_id,
  k = "",
  level = 1,
  scratch = 0,
}) {
  // console.log(k, refhid, { father_id, mother_id });
  if (!father_id && !mother_id) return [];
  let ar = [];
  let ftree = [];
  let fet_ftree = scratch == 1 ? undefined : await find_hid_tree(father_id);
  if (fet_ftree !== undefined) {
    ftree = fet_ftree;
    ftree = ftree.map(([hid, k, level]) => {
      return [hid, "f" + k, level + 1];
    });
  } else {
    let fparents = await get_parents(father_id);
    if (fparents) {
      ar.push([fparents.father, k + "f" + "f", level]);
      ar.push([fparents.mother, k + "f" + "m", level]);
    }
    ftree =
      fparents == null
        ? []
        : await get_ancesters_stub({
            refhid: father_id,
            father_id: fparents.father,
            mother_id: fparents.mother,
            k: k + "f",
            level: level + 1,
            scratch,
          });
  }

  let mtree = [];
  let fet_mtree = scratch == 1 ? undefined : await find_hid_tree(mother_id);
  if (fet_mtree !== undefined) {
    mtree = fet_mtree;
    mtree = mtree.map(([hid, k, level]) => {
      return [hid, "m" + k, level + 1];
    });
  } else {
    let mparents = await get_parents(mother_id);
    if (mparents) {
      ar.push([mparents.father, k + "m" + "f", level]);
      ar.push([mparents.mother, k + "m" + "m", level]);
    }
    mtree =
      mparents == null
        ? []
        : await get_ancesters_stub({
            refhid: mother_id,
            father_id: mparents.father,
            mother_id: mparents.mother,
            k: k + "m",
            level: level + 1,
            scratch,
          });
  }
  ar = [...ar, ...ftree, ...mtree];
  ar = _.sortBy(ar, 2);
  ar = _.compact(ar);
  return ar;
}
async function find_hid_tree(hid) {
  let doc = await zed_db.db.collection("line").findOne({ hid });
  let tree = getv(doc, "tree");
  if (tree) {
    console.log("found tree for", hid);
    return tree;
  }
  return undefined;
}
async function get_ancesters({ hid }) {
  let tree = [];
  tree = await find_hid_tree(hid);
  if (tree != undefined) return tree;

  let parents = await get_parents(hid);
  if (!parents) return [];
  let ans = await get_ancesters_stub({
    father_id: parents.father,
    mother_id: parents.mother,
    k: "",
    level: 2,
  });
  tree = [[parents.father, "f", 1], [parents.mother, "m", 1], ...ans];
  return tree;
}
const ancestors = { get_ancesters, get_ancesters_stub };
module.exports = ancestors;
