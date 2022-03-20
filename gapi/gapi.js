const fs = require("fs");
const { google } = require("googleapis");
const path = require("path");
require("dotenv").config();

const secret_path = process.env.secret_path;
const secret = process.env.secret;
const scopes = [
  "https://www.googleapis.com/auth/cloud-platform",
  "https://www.googleapis.com/auth/spreadsheets",
];

let auth;

const init = async () => {
  try {
    let act_path = path.resolve(__dirname, secret_path);
    if (fs.existsSync(act_path));
    else {
      let secret_ob = JSON.parse(secret);
      console.log("project_id:", secret_ob.project_id);
      fs.writeFileSync(act_path, JSON.stringify(secret_ob));
      console.log("created gapi service account json credentials");
    }
    auth = new google.auth.GoogleAuth({ keyFile: act_path, scopes });
    const authClient = await auth.getClient();
    google.options({ auth: authClient });
  } catch (err) {
    console.log("err at gapi", err);
  }
};

const gapi = {
  auth,
  init,
};
module.exports = gapi;
