// src/routes/memberRoutes.js
const express = require('express');
const router = express.Router();
const memberController = require('../controllers/memberController');

router.get('/members', memberController.listMembers);
router.post('/members', memberController.createMember); // เพิ่มบรรทัดนี้
router.put('/members/:memberId', memberController.updateMember); // เพิ่มบรรทัดนี้

module.exports = router;