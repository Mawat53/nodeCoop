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
  // 1. ตรวจสอบและเริ่มต้น Oracle Client (แยก try/catch เพื่อให้ Server รันต่อได้)
  try {
    await db.initClient();
    console.log("✅ Oracle Client initialized successfully.");
  } catch (clientError) {
    console.error(
      "⚠️ WARNING: Error initializing Oracle Client. Database functionality may be limited or unavailable."
    );
    console.error("Client Error Details:", clientError.message);
    // ไม่สั่ง process.exit(1)
  }

  // 2. สร้าง Connection Pool เพียงครั้งเดียว (Pool Initialization)
  try {
    // หาก db.initClient() สำเร็จ โค้ดส่วนนี้จะสร้าง Pool
    // หาก db.initClient() ล้มเหลว โค้ดส่วนนี้อาจล้มเหลวด้วย ORA-XXXXX
    await db.initialize();
    console.log("✅ Database connection pool initialized and ready.");
  } catch (dbError) {
    // จัดการ Error เช่น ORA-12516 หรือ Error ที่เกิดจากการต่อฐานข้อมูล
    console.error(
      "⚠️ WARNING: Failed to initialize database connection pool. DB operations will fail."
    );
    console.error("Database Error Details:", dbError.message);
    // ไม่สั่ง process.exit(1)
  }

  // 3. เริ่ม Server (ทำทุกครั้ง ไม่ว่าจะต่อ DB ได้หรือไม่)
  try {
    app.listen(PORT, () => {
      console.log(`🚀 Server is running on http://localhost:${PORT}`);
    });
  } catch (err) {
    console.log("🔴 Fatal Error: Failed to start server listening on port.", err);
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
