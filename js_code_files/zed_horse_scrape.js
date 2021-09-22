const { driver_test } = require("./webdriver-utils");
const kyh_url = (hid) => `https://knowyourhorses.com/horses/${hid}`;
const hawku_url = (hid) => `https://www.hawku.com/horse/${hid}`;

const scrape_horse_details = () => {
  let driver;
  try {
    driver = "";
  } catch (err) {
    console.log("err on scrape_horse_details", hid);
  }
};

const runner = async () => {
  console.log("runner");
  await driver_test();
  console.log("done");
};
runner();
