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
  // 1. ตรวจสอบการเชื่อมต่อ Oracle Client โดยแยก try/catch
    try {
        await db.initClient();
        console.log("✅ Oracle Client initialized successfully.");
    } catch (clientError) {
        // หากเกิด Error DPI-1047 หรือ Error อื่น ๆ ที่เกี่ยวข้อง
        console.error("⚠️ WARNING: Error initializing Oracle Client. Database functionality may be limited or unavailable.");
        console.error("Client Error Details:", clientError.message);
        // *** สำคัญ: ไม่เรียก process.exit(1) ที่นี่ ***
        // *** โปรแกรมจะดำเนินการต่อ ***
    }
    // 2. ลองเชื่อมต่อฐานข้อมูล (ถ้า initClient ผ่านหรือเรายอมให้มันลองต่อ)
    // การเรียก db.initialize() อาจจะล้มเหลวหาก client initialization ล้มเหลว แต่เรายังให้ server รันต่อ
    try {
        await db.initialize();
        console.log("✅ Database connection pool initialized.");
    } catch (dbError) {
        console.error("⚠️ WARNING: Failed to initialize database connection pool.");
        console.error("Database Error Details:", dbError.message);
        // Server ยังคงรันต่อแม้ว่า DB จะต่อไม่สำเร็จ
    }
  // 3. สั่งให้ Server รันต่อเสมอ
    try {
        app.listen(PORT, () => {
            console.log(`🚀 Server is running on http://localhost:${PORT}`);
        });
    } catch (serverError) {
        // หาก Server มีปัญหาในการ Listen Port จริง ๆ (เช่น Port ถูกใช้)
        console.error("🔴 Fatal Error: Failed to start server listening on port.", serverError);
        process.exit(1); // หยุดโปรแกรมหาก Server รันไม่ได้จริง ๆ
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
