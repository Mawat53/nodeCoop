// src/config/database.js
const oracledb = require("oracledb");
require("dotenv").config();

// เพิ่มบรรทัดนี้เพื่อตั้งค่า Thin mode
oracledb.thin = true;

// การตั้งค่าการเชื่อมต่อฐานข้อมูล

const dbConfig = {
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  connectString: process.env.DB_CONNECTION_STRING,
  poolMin: 4, // จำนวน connection ขั้นต่ำ
  poolMax: 10, // จำนวน connection สูงสุด
  poolIncrement: 2, // จำนวน connection ที่จะเพิ่มเมื่อจำเป็น
};

let pool; // <--- ตัวแปรสำหรับเก็บ Connection Poll

// เพิ่มฟังก์ชันนี้เพื่อจัดการการตั้งค่า client
async function initClient() {
  try {
    oracledb.initOracleClient({
      libDir: process.env.ORACLE_CLIENT_LIB_DIR,
    });
    console.log("Oracle Client initialized successfully.");
  } catch (err) {
    console.error("Error initializing Oracle Client: ", err.message);
    throw new Error("Error initializing Oracle Client: " + err.message);
  }
}

async function initialize() {
  // 1. ตรวจสอบว่า Pool ถูกสร้างแล้วหรือยัง
  if (pool) {
    console.log("Pool already initialized. Skipping creation.");
    return;
  }

  // 2. สร้าง Pool โดยใช้ dbConfig ทั้งก้อน
  // oracledb.createPool() จะรับคุณสมบัติทั้งหมดใน dbConfig ไปใช้
  try {
    // โค้ดสร้าง Pool จะรันเพียงครั้งเดียวเมื่อ pool เป็น null/undefined
    pool = await oracledb.createPool(dbConfig);
    console.log("Connection pool to Oracle created!");
  } catch (err) {
    console.error("Error creating connection pool: ", err);
    throw new Error("Error creating connection pool: " + err.message);
  }
}

function getPool() {
  // ฟังก์ชันนี้ให้ Pool ที่ถูกสร้างแล้วนำไปใช้ใน Routes/Services
  if (!pool) {
    throw new Error("Database connection pool has not been initialized.");
  }
  return pool;
}

function getPool() {
    // ฟังก์ชันนี้ให้ Pool ที่ถูกสร้างแล้วนำไปใช้ใน Routes/Services
    if (!pool) {
        throw new Error("Database connection pool has not been initialized.");
    }
    return pool;
}


async function close() {
  try {
    await oracledb.getPool().close();
    console.log("Connection pool to Oracle closed.");
  } catch (err) {
    console.error("Error closing connection pool:", err);
  }
}

async function getConnection() {
  return await oracledb.getPool().getConnection();
}

module.exports = {
  initClient,
  initialize,
  close,
  getConnection,
  getPool,
};
