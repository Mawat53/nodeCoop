// src/routes/memberRoutes.js
const express = require('express');
const router = express.Router();
const memberController = require('../controllers/memberController');

// Route สำหรับเรียกดูข้อมูลสมาชิกทั้งหมด
router.get('/members', memberController.listMembers);
// Route สำหรับเพิ่มข้อมูลสมาชิกใหม่
router.post('/members', memberController.createMember); // เพิ่มบรรทัดนี้
// Route สำหรับอัปเดตข้อมูลสมาชิก (ระบุด้วย memberId)
router.put('/members/:memberId', memberController.updateMember); // เพิ่มบรรทัดนี้

module.exports = router;