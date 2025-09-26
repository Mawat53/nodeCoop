//src/routes/loancontractRouters.js
const express = require("express");
const router = express.Router();
const loancontractController = require("../controllers/loancontractControll");

router.get("/loancontracts", loancontractController.listLoanContracts);
router.post("/process-interest-return", loancontractController.processInterestReturn);
router.get('/generate-report', loancontractController.generateReport);


module.exports = router;