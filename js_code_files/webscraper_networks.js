const { Browser, Builder, until, By } = require("selenium-webdriver");
const { Options, ServiceBuilder } = require("selenium-webdriver/chrome");
const { get_webdriver } = require("./webdriver-utils");
require("dotenv").config();

const requests_webscraper = async () => {
  console.log("driver");
  try {
    let driver = await get_webdriver();
    let url = "https://zed.run/race/NMUPsYw";
    await driver.get(url);
    await driver.sleep(2000);
  } catch (err) {
    console.log("err", err.message);
  }
};

const runner = async () => {
  await requests_webscraper();
  console.log("done");
};
// runner();
