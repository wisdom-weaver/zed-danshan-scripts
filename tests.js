const { generate_blood_mapping } = require("./index-odds-generator");
const { init } = require("./index-run");

const test1 = async () => {
  await init()
  console.log("test1");
  await generate_blood_mapping()
};
module.exports = {
  test1,
};
