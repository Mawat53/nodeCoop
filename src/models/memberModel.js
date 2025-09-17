// src/models/memberModel.js
const db = require("../config/database");

async function getAllMembers() {
  let connection;
  try {
    connection = await db.getConnection();
    const result = await connection.execute(
      `SELECT MEMBER_NO, MEMB_NAME, MEMB_SURNAME FROM MBMEMBMASTER WHERE MEMBER_NO = '023999'`
    );
    // console.log("result.rows", result.rows);
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
async function createMember(member) {
  let connection;
  try {
    connection = await db.getConnection();
    const sql = `
            INSERT INTO MEMBERS (MEMBER_ID, NAME, EMAIL, ADDRESS, JOIN_DATE)
            VALUES (:memberId, :name, :email, :address, SYSDATE)
        `;
    const result = await connection.execute(
      sql,
      {
        memberId: member.memberId,
        name: member.name,
        email: member.email,
        address: member.address,
      },
      { autoCommit: true } // autoCommit: true เพื่อบันทึกข้อมูลทันที
    );
    return result.rowsAffected;
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
async function updateMember(memberId, member) {
  let connection;
  try {
    connection = await db.getConnection();
    const sql = `
            UPDATE MBMEMBMASTER
            SET MEMB_SURNAME = :membSurname
            WHERE MEMBER_NO = :memberId
        `;
    console.log("member:", member);
    const result = await connection.execute(
      sql,
      {
        memberId: memberId,
        membSurname: member.membSurname,
      },
      { autoCommit: true }
    );
    return result.rowsAffected;
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
  getAllMembers,
  createMember,
  updateMember,
};
