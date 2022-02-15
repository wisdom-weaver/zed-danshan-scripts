const knex = require("knex");
// const to_knex = require("postgresql-to-knex");
require("dotenv").config();

const conn = {
  client: "pg",
  connection: {
    host: process.env.PG_URI,
    port: 5432,
    user: "postgres",
    password: process.env.PG_PASS,
    database: "postgres",
  },
};
const knex_conn = knex(conn);
module.exports = {
  knex_conn,
  conn,
  // to_knex,
};
