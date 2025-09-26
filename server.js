// server.js
const express = require("express");
const db = require("./src/config/database");
const memberRoutes = require("./src/routes/memberRoutes");
const shareRoutes = require("./src/routes/shareRoutes");
const loancontractRouters = require("./src/routes/loancontractRouters");
require("dotenv").config();

const app = express();
const PORT = process.env.PORT || 3001;

// เพิ่มโค้ดส่วนนี้
// 1. เรียกใช้ db.initClient() เพื่อตั้งค่า Instant Client
// 2. เรียกใช้ db.initialize() เพื่อสร้าง Connection Pool
async function startServerDB() {
  try {
    await db.initClient(); // เรียกใช้ฟังก์ชั่น initClient ที่คุณเพิ่มเข้าไป
    await db.initialize();
    app.listen(PORT, () => {
      console.log(`Server is running on http://localhost:${PORT}`);
    });
  } catch (err) {
    console.log("Failed to start server:", err);
    process.exit(1);
  }
}

app.use(express.json());

// Routes
app.use("/api", memberRoutes, shareRoutes, loancontractRouters);

startServerDB();

process.on("SIGINT", () => {
  db.close().then(() => {
    process.exit();
  });
});
