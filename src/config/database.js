// src/config/database.js
const oracledb = require('oracledb');
require('dotenv').config();


// เพิ่มบรรทัดนี้เพื่อตั้งค่า Thin mode
oracledb.thin = true;
oracledb.initOracleClient({
  libDir: "C:\\oracle\\instantclient_21_12\\instantclient_23_7",
}); // Windows enable single row


const dbConfig = {
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    connectString: process.env.DB_CONNECTION_STRING
};

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
    initialize,
    close,
    getConnection
};