const { Browser, Builder, until, By } = require("selenium-webdriver");
const chrome = require('selenium-webdriver/chrome');
const chromedriver = require("chromedriver");
const { Options, ServiceBuilder } = chrome;
const {delay} = require("./utils")
require("dotenv").config();
let options = new Options();

// This tells Selenium where to find your Chrome browser executable
// options.setChromeBinaryPath(process.env.CHROME_BINARY_PATH);
console.log(chromedriver.path);
chrome.setDefaultService(new chrome.ServiceBuilder(chromedriver.path).build());

// These options are necessary if you'd like to deploy to Heroku
// options.addArguments("--headless");
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
    // await delay(2000);
    return driver;
  } catch (err) {
    console.log("err in get_webdriver");
    console.log(err);
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
    console.log("# webdriver not working");
    console.log(err);
  }
};

const elem_by_x = async (driver, x_path) => {
  return driver.findElement(By.xpath(x_path));
};
const elems_by_x = async (driver, x_path) => {
  return driver.findElements(By.xpath(x_path));
};
const elem_by_x_txt = async (driver, x_path) => {
  try {
    let elem = await driver.findElement(By.xpath(x_path));
    let txt = await elem.getText();
    return txt;
  } catch (err) {
    return "";
  }
};

module.exports = {
  driver_test,
  get_webdriver,
  elem_by_x,
  elem_by_x_txt,
  elems_by_x,
};
