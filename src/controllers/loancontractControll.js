//src/controllers/loancontractController.js
const loancontractModel = require("../models/loancontractModel");
const ExcelJS = require("exceljs");

async function listLoanContracts(req, res) {
  try {
    const loanContracts = await loancontractModel.getAllLoanContracts();
    res.status(200).json(loanContracts);
  } catch (err) {
    console.error("Error processing list all loan contract:", err.message);
    res
      .status(500)
      .json({
        error: "Failed to retrieve all loan contracts",
        details: err.message,
      });
  }
}

async function processInterestReturn(req, res) {
  try {
    const result = await loancontractModel.processInterestReturn();
    res.status(200).json(result);
  } catch (err) {
    console.log("Error processing interest return:", err.message);
    res
      .status(500)
      .json({
        error: "Failed to process interest return",
        details: err.message,
      });
  }
}

async function generateReport(req, res) {
  try {
    // const { dataForReport } = await loancontractModel.processInterestReturnAndGenerateReport();
    const result =
      await loancontractModel.processInterestReturnAndGenerateReport();
    // ตรวจสอบว่ามีข้อมูลสำหรับสร้างรายงานหรือไม่
    if (result.processedCount === 0) {
      return res
        .status(200)
        .json({ processedCount: 0, message: result.message, status: false });
    }

    const dataForReport = result.dataForReport;

    // ตรวจสอบว่ามีข้อมูลสำหรับรายงานหรือไม่
    if (dataForReport.processedCount === 0) {
      return res.status(200).json({ message: result.message });
    }
    // สร้าง Workbook และ Worksheet
    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet("Interest Return Report");

    // กำหนด Header
    worksheet.columns = [
      { header: "MEMBER_NO", key: "MEMBER_NO", width: 15 },
      { header: "LOANCONTRACT_NO", key: "LOANCONTRACT_NO", width: 20 },
      { header: "PRINCIPAL_BALANCE", key: "PRINCIPAL_BALANCE", width: 20 },
      { header: "INTEREST_RETURN", key: "INTEREST_RETURN", width: 20 },
      { header: "NEWCONT_NO", key: "NEWCONT_NO", width: 20 },
      { header: "NEWPRINCIPAL_BALANCE", key: "NEWPRINCIPAL_BALANCE", width: 20 },
      { header: "REMARK", key: "REMARK", width: 50 },
    ];

    // เพิ่มข้อมูล
    worksheet.addRows(dataForReport);

    // ตั้งค่า Response Header เพื่อส่งไฟล์ Excel
    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    );
    res.setHeader(
      "Content-Disposition",
      "attachment; filename=" + "Interest_Return_Report.xlsx"
    );

    // เขียน Workbook ลงใน Response Stream
    await workbook.xlsx.write(res);
    res.end();
  } catch (err) {
    console.error("Error generating report:", err.message);
    res
      .status(500)
      .json({ error: "Failed to generate report", details: err.message });
  }
}

module.exports = {
  listLoanContracts,
  processInterestReturn,
  generateReport,
};
