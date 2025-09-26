// src/config/database.js
const oracledb = require('oracledb');
require('dotenv').config();


// เพิ่มบรรทัดนี้เพื่อตั้งค่า Thin mode
oracledb.thin = true;
// oracledb.initOracleClient({
//   libDir: "C:\\oracle\\instantclient_21_12\\instantclient_23_7",
// }); // Windows enable single row


const dbConfig = {
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    connectString: process.env.DB_CONNECTION_STRING,
    poolMin: 4, // จำนวน connection ขั้นต่ำ
    poolMax: 10, // จำนวน connection สูงสุด
    poolIncrement: 2, // จำนวน connection ที่จะเพิ่มเมื่อจำเป็น

};

// เพิ่มฟังก์ชันนี้เพื่อจัดการการตั้งค่า client
async function initClient() {
    try {
        oracledb.initOracleClient({
            libDir: process.env.ORACLE_CLIENT_LIB_DIR,
        });
        console.log('Oracle Client initialized successfully.');
    } catch (err) {
        console.error('Error initializing Oracle Client:', err.message);
        throw err;
    }
}

async function initialize() {
    try {
        await oracledb.createPool(dbConfig);
        console.log('Connection pool to Oracle created!');
    } catch (err) {
        console.error('Error creating connection pool:', err);
    }
}

async function close() {
    try {
        await oracledb.getPool().close();
        console.log('Connection pool to Oracle closed.');
    } catch (err) {
        console.error('Error closing connection pool:', err);
    }
}

async function getConnection() {
    return await oracledb.getPool().getConnection();
}

module.exports = {
    initClient,
    initialize,
    close,
    getConnection
};