// src/models/shareModel.js
const db = require("../config/database");

async function getAllShares() {
  let connection;
  try {
    connection = await db.getConnection();
    const result = await connection.execute(
      `SELECT * FROM SHSHAREMASTER WHERE MEMBER_NO = '023999'`
    );
    console.log("result.rows", result.rows);
    return result.rows;
  } catch (err) {
    throw new Error(err);
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (err) {
        console.error(err);
      }
    }
  }
}

module.exports = {
  getAllShares,
};
