// src/routes/shareRoutes.js
const express = require('express');
const router = express.Router();
const shareController = require('../controllers/shareController');

router.get('/shares', shareController.listShares);

module.exports = router