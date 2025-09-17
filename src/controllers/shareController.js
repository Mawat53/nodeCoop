// src/controllers/shareController.js
const shareModel = require("../models/shareModel");

async function listShares(req, res) {
  try {
    const shares = await shareModel.getAllShares();
    const dataFormat = shares.map((share) => {
        
    });
    res
      .status(200)
      .json({
        message: "ค้นหาสำเร็จ",
        success: true,
        countAll: shares.length,
        data: shares,
      });
  } catch (err) {
    console.error("Error in shareController:", err.message);
    res.status(500).json({ error: "Failed to retrieve shares" });
  }
}

module.exports = {
  listShares,
};
