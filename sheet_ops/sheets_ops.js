const _ = require("lodash");
const { google } = require("googleapis");

const get_sheet = async ({ spreadsheetId, range }) => {
  try {
    const sheets = google.sheets({ version: "v4" });
    let data = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range,
    });
    return data.data.values;
  } catch (err) {
    console.log("get_sheet", "\n", err);
    return {};
  }
};

const push_to_sheet = async ({ spreadsheetId, range, values }) => {
  try {
    const sheets = google.sheets({ version: "v4" });
    const data = await sheets.spreadsheets.values.update({
      // includeValuesInResponse: true,
      includeValuesInResponse: false,
      range,
      spreadsheetId,
      // responseValueRenderOption: "FORMATTED_VALUE",
      // responseValueRenderOption: "FORMULA",
      valueInputOption: "USER_ENTERED",
      requestBody: {
        majorDimension: "ROWS",
        range,
        values,
      },
    });
    return data;
  } catch (err) {
    console.log("push_to_sheet", "\n", err);
    return {};
  }
};

const test = async () => {
  try {
  } catch (err) {
    console.log("push_to_sheet", "\n", err);
    return {};
  }
};

const struct_ob_to_values = (data, wkeys = true) => {
  let keys = _.keys(data[0]);
  console.log(keys);
  let ar = _.map(data, _.values);
  let values = wkeys ? [keys, ...ar] : ar;
  return values;
};

const sheet_print_ob = async (ob, { range, spreadsheetId }, wkeys = true) => {
  let values = struct_ob_to_values(ob, wkeys);
  let conf = { range, spreadsheetId, values };
  return push_to_sheet(conf);
};
const sheet_print_cell = async (val, { range, spreadsheetId }) => {
  let values = [[val]];
  let conf = { range, spreadsheetId, values };
  return push_to_sheet(conf);
};

const sheet_ops = {
  get_sheet,
  push_to_sheet,
  test,
  sheet_print_ob,
  sheet_print_cell,
};

module.exports = sheet_ops;
