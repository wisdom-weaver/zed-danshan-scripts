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

const sheet_ops = {
  get_sheet,
  push_to_sheet,
  test,
};

module.exports = sheet_ops;
