const { Browser, Builder, until } = require("selenium-webdriver");
const { Options, ServiceBuilder } = require("selenium-webdriver/chrome");
require("dotenv").config();

let options = new Options();

// This tells Selenium where to find your Chrome browser executable
options.setChromeBinaryPath(process.env.CHROME_BINARY_PATH);

// These options are necessary if you'd like to deploy to Heroku
options.addArguments("--headless");
options.addArguments("--disable-gpu");
options.addArguments("--no-sandbox");
options.addArguments("start-maximized");

const get_webdriver = async () => {
  try {
    let serviceBuilder = new ServiceBuilder(process.env.CHROME_DRIVER_PATH);
    let driver = new Builder()
      .forBrowser(Browser.CHROME)
      .setChromeOptions(options)
      .setChromeService(serviceBuilder)
      .build();
    return driver;
  } catch (err) {
    console.log("err in get_webdriver");
    return null;
  }
};

const driver_test = async () => {
  try {
    let driver = await get_webdriver();
    await driver.get("http://www.google.com");
    let title = await driver.getTitle();
    if (title == "Google") console.log("# webdriver active");
    else console.log("# webdriver active");
    await driver.quit();
  } catch (err) {
    console.log("# webdriver not working", err);
  }
};

module.exports = {
  driver_test,
  get_webdriver,
};
