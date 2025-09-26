//src/models/loancontractModel.js
const db = require("../config/database");
const oracledb = require("oracledb");

async function getAllLoanContracts() {
  let connection;
  try {
    connection = await db.getConnection();
    const result = await connection.execute(
      `SELECT MEMBER_NO, LOANCONTRACT_NO, PRINCIPAL_BALANCE, INTEREST_RETURN FROM LNCONTMASTER WHERE PRINCIPAL_BALANCE > 0 AND MEMBER_NO = '023999'`
    );
    console.log("result.rows", result.rows);
    return result.rows;
  } catch (err) {
    throw new Error(err);
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (err) {
        console.error(err);
      }
    }
  }
}

async function getInterestReturnMember() {
  let connection;
  try {
    connection = await db.getConnection();
    const result = await connection.execute(
      `SELECT MEMBER_NO, LOANCONTRACT_NO, PRINCIPAL_BALANCE, INTEREST_RETURN FROM LNCONTMASTER WHERE PRINCIPAL_BALANCE > 0 AND MEMBER_NO = '023999'`
    );
    console.log("result.rows", result.rows);
    return result.rows;
  } catch (err) {
    throw new Error(err);
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (err) {
        console.error(err);
      }
    }
  }
}

// Function สมมติ: คำนวณดอกเบี้ยล่าสุด
// ในความเป็นจริงต้องเขียน Logic คำนวณตามเงื่อนไขทางธุรกิจ
async function calInterateLoan(connection, contractNo) {
  // นี่คือ Logic การคำนวณดอกเบี้ย
  // ตัวอย่าง: ดึงข้อมูลที่จำเป็นจากฐานข้อมูลแล้วนำมาคำนวณ
  const interestSql = `
        SELECT NVL(principal_balance, 0) * (6 / 100) / 365 AS calculated_interest
        FROM lncontmaster WHERE trim(loancontract_no) = :contractNo
    `;
  const result = await connection.execute(interestSql, { contractNo });
  const interest =
    result.rows.length > 0 ? result.rows[0].CALCULATED_INTEREST : 0;
  return interest;
}

// Function สมมติ: Insert ข้อมูลลง lncontstatement
async function insertLoanStatement(
  connection,
  loanData,
  paymentType,
  amount,
  refNo
) {
  // 1. ดึงค่า principal_balance ปัจจุบันของสัญญา
  const currentPrincipalSql = `SELECT principal_balance FROM lncontmaster WHERE trim(loancontract_no) = :contractNo`;
  const currentPrincipalResult = await connection.execute(
    currentPrincipalSql,
    {
      contractNo: loanData.LOANCONTRACT_NO,
    },
    {
      outFormat: oracledb.OUT_FORMAT_OBJECT,
    }
  );
  const currentPrincipal =
    currentPrincipalResult.rows.length > 0
      ? currentPrincipalResult.rows[0].PRINCIPAL_BALANCE
      : 0;

  // 2. คำนวณ principal_balance ใหม่
  const newPrincipalBalance =
    paymentType === "principal" ? currentPrincipal - amount : currentPrincipal;

  const insertSql = `
        INSERT INTO lncontstatement (
            LOANCONTRACT_NO, SEQ_NO, LOANITEMTYPE_CODE, SLIP_DATE, OPERATE_DATE, REF_DOCNO, PERIOD,
            PRINCIPAL_PAYMENT, INTEREST_PAYMENT, PRINCIPAL_BALANCE, BFINTARREAR_AMT, INTEREST_PERIOD,
            INTEREST_ARREAR, INTEREST_RETURN, ITEM_STATUS, ENTRY_ID, ENTRY_DATE, COOPBRANCH_ID
        ) VALUES (
            :contractNo, (SELECT NVL(MAX(SEQ_NO), 0) + 1 FROM lncontstatement WHERE LOANCONTRACT_NO = :contractNo),
            'LRT', TRUNC(SYSDATE), TRUNC(SYSDATE),
             :refNo, 0, 
            :principalPayment, :interestPayment,:prinBal,0, 0,
            0,0,1,'วัชริศ',SYSDATE,'001'
        )
    `;
  const bindData = {
    contractNo: loanData.LOANCONTRACT_NO,
    slipDate: new Date(),
    operateDate: new Date(),
    refNo: refNo, // <--- ใช้ refNo ที่ส่งเข้ามา
    principalPayment: paymentType === "principal" ? amount : 0,
    interestPayment: paymentType === "interest" ? amount : 0,
    prinBal: newPrincipalBalance, // <--- ใช้ principal_balance ใหม่
  };
  await connection.execute(insertSql, bindData);
}

// Function สมมติ: Update ข้อมูลใน lncontmaster
async function updateLoanMaster(
  connection,
  loanData,
  principalPaid,
  interestPaid
) {
  const updateSql = `
        UPDATE lncontmaster 
        SET 
            INTEREST_RETURN = 0,
            LAST_STM_NO = (SELECT NVL(MAX(SEQ_NO), 0) FROM lncontstatement WHERE LOANCONTRACT_NO = :contractNo),
            lastcalint_date = SYSDATE,
            principal_balance = (principal_balance - :principalPaid)
        WHERE trim(loancontract_no) = :contractNo
    `;
  const bindData = {
    principalPaid: principalPaid,
    interestPaid: interestPaid, // ใช้ interestPaid ในการลดดอกเบี้ยค้าง
    contractNo: loanData.LOANCONTRACT_NO,
  };
  await connection.execute(updateSql, bindData);
}
/**
 * Handles the complete transaction for creating a loan statement.
 * Fetches the next document number, inserts the statement, and updates the document counter.
 *
 * @param {object} connection - The OracleDB connection object.
 * @param {object} loanData - The loan data.
 * @param {string} paymentType - The payment type ('principal' or 'interest').
 * @param {number} amount - The payment amount.
 */
async function createLoanStatementTransaction(
  connection,
  loanData,
  paymentType,
  amount
) {
  try {
    // 1. Get the current document number from cmshrlondoccontrol.
    const refNoSql = `SELECT last_documentno FROM cmshrlondoccontrol WHERE document_code = 'CMSLIPRECEIPT' FOR UPDATE`;
    const refNoResult = await connection.execute(refNoSql, [], {
      outFormat: oracledb.OUT_FORMAT_OBJECT,
    });
    const currentRefNo =
      refNoResult.rows.length > 0 ? refNoResult.rows[0].LAST_DOCUMENTNO : null;

    if (!currentRefNo) {
      throw new Error(
        "Failed to get a valid document number from cmshrlondoccontrol."
      );
    }

    const newRefNo = (parseInt(currentRefNo) + 1)
      .toString()
      .padStart(currentRefNo.length, "0");

    // 2. Call the original insertLoanStatement function with the new document number.
    // **แก้ไขตรงนี้:** เพิ่ม newRefNo เป็นพารามิเตอร์
    await insertLoanStatement(connection, loanData, paymentType, amount, newRefNo);

    // 3. If the insert is successful, update the document counter.
    const updateDocNoSql = `
            UPDATE cmshrlondoccontrol
            SET last_documentno = :newRefNo
            WHERE document_code = 'CMSLIPRECEIPT'
        `;
    await connection.execute(updateDocNoSql, { newRefNo });
  } catch (error) {
    console.error("Error in createLoanStatementTransaction:", error);
    throw error; // Re-throw the error to be handled by the main function's catch block.
  }
}

/**
 * จัดการ Logic การชำระเงินสำหรับสัญญาเงินกู้
 * @param {object} connection - OracleDB connection object
 * @param {object} loan - ข้อมูลสัญญาเงินกู้ที่ต้องการประมวลผล
 * @param {number} interestReturnAmount - จำนวนเงินคืนดอกเบี้ย
 * @returns {object} { loanUpdated, remarks } - ข้อมูลสัญญาที่อัปเดตและข้อความหมายเหตุ
 */
async function processPaymentLogic(connection, loan, interestReturnAmount) {
  let remarks = "";
  let principalPaid = 0;
  let interestPaid = 0;
  const loanUpdated = { ...loan };

  // 1. Check the last interest calculation date.
  const checkDateSql = `SELECT TRUNC(lastcalint_date) AS LASTCALINT_DATE, LAST_STM_NO FROM  FROM lncontmaster WHERE trim(loancontract_no) = :contractNo`;
  const dateResult = await connection.execute(checkDateSql, {
    contractNo: loan.LOANCONTRACT_NO,
  });

  if (dateResult.rows.length === 0) { throw new Error(`Loan contract ${loan.LOANCONTRACT_NO} not found.`); }
    const { LASTCALINT_DATE: lastCalIntDate } = dateResult.rows[0];

  const today = new Date();
  today.setHours(0, 0, 0, 0);

  if (lastCalIntDate && lastCalIntDate.getTime() === today.getTime()) {
    // Case 1: Date is today. Pay principal with the return amount.
    principalPaid = interestReturnAmount;
    remarks = `เงินคืนตรงกับสัญญา ${loan.LOANCONTRACT_NO} และวันที่คิดดอกเบี้ยล่าสุดตรงกับวันนี้ จะนำเงินคืนไปชำระเงินต้นทั้งหมด`;

    // Use the new transaction function here.
    await createLoanStatementTransaction(connection, loan, 'principal', principalPaid);
        await updateLoanMaster(connection, loan, principalPaid, 0); // principalPaid, interestPaid
        loanUpdated.principal_balance -= principalPaid;


  } else {
    // Case 2: Date is not today. Calculate and pay interest first.
    const calculatedInterest = await calInterateLoan(connection, loan.LOANCONTRACT_NO);

    // Use the new transaction function to insert the interest payment.
    interestPaid = Math.min(interestReturnAmount, calculatedInterest);
        const remainingReturn = interestReturnAmount - interestPaid;


    if (remainingReturn > 0) {
      // If money is left, pay the principal.
      principalPaid = remainingReturn;
      remarks = `เงินคืนตรงกับสัญญา ${loan.LOANCONTRACT_NO} แต่วันที่คิดดอกเบี้ยไม่ตรง จะนำเงินคืนไปชำระดอกเบี้ย ${interestPaid} และที่เหลือ ${principalPaid} จะนำไปชำระเงินต้น`;

      // Use the new transaction function to insert the principal payment.
      await createLoanStatementTransaction(connection, loan, 'principal', principalPaid);
            await updateLoanMaster(connection, loan, principalPaid, interestPaid); // principalPaid, interestPaid
            loanUpdated.principal_balance -= principalPaid;

    } else {
      // Not enough money to pay off the calculated interest.
      remarks = `เงินคืนตรงกับสัญญา ${loan.LOANCONTRACT_NO} แต่วันที่คิดดอกเบี้ยไม่ตรง และเงินคืน ${interestReturnAmount} ไม่เพียงพอสำหรับชำระดอกเบี้ยที่คำนวณได้ ${calculatedInterest}`;

      // Update the loan master record, paying what we can on interest.
      await updateLoanMaster(connection, loan, 0, interestPaid); // principalPaid, interestPaid
    }
  }

  return { loanUpdated, remarks };
}

async function processInterestReturn() {
  let connection;
  try {
    connection = await db.getConnection();

    // const selectSql = `
    //         SELECT trim(sl.MEMBER_NO) as MEMBER_NO, trim(sd.LOANCONTRACT_NO) as LOANCONTRACT_NO , sd.INTEREST_RETURN as INTEREST_RETURN , sl.ref_newcontno as NEWCONT_NO , nlmn.principal_balance as NEWPRINCIPAL_BALANCE
    //         FROM cmshrlonslip sl
    //         JOIN cmshrlonslipdet sd on sl.slip_no = sd.slip_no
    //         JOIN mbmembmaster mb on sl.member_no = mb.member_no
    //         JOIN mbucfprename mpre on mb.prename_code = mpre.prename_code
    //         LEFT JOIN mbucfmembgroup mgrp on mb.membgroup_code = mgrp.membgroup_code
    //         LEFT JOIN lncontmaster nlmn on trim(nlmn.loancontract_no) = trim(sl.ref_newcontno)
    //         WHERE sl.slip_date between to_date('01/09/2025','dd/mm/yyyy') and to_date('30/09/2025','dd/mm/yyyy')
    //         AND sl.sliptype_code in ('PX','CLC')
    //         AND sd.slipitemtype_code = 'LON'
    //         AND sd.interest_return > 0
    //         AND sl.slip_status = 1
    //         AND sl.member_no = '023999' //fortest
    //     `;
    const selectSql = `select member_no, loancontract_no, interest_return, 'สป6802227' as newcont_no, 691000 as newprincipal_balance 
from lncontmaster where member_no = '023999' and loancontract_no = 'สป6700486' `;
    const result = await connection.execute(selectSql, [], {
      outFormat: oracledb.OUT_FORMAT_OBJECT,
    });
    const loansToProcess = result.rows;

    if (loansToProcess.length === 0) {
      return {
        processedCount: 0,
        message: "No loans found with outstanding interest return.",
        status: false,
      };
    }

    const positivePrincipalLoans = await getPositivePrincipalLoans(connection);

    const loansByMember = new Map();
    for (const loan of positivePrincipalLoans) {
      if (!loansByMember.has(loan.MEMBER_NO)) {
        loansByMember.set(loan.MEMBER_NO, []);
      }
      loansByMember.get(loan.MEMBER_NO).push(loan);
    }

    const principalBalanceMap = new Map(
      positivePrincipalLoans.map((loan) => [
        loan.LOANCONTRACT_NO,
        loan.PRINCIPAL_BALANCE,
      ])
    );

    const dataForReport = [];

    for (const loan of loansToProcess) {
      const memberLoans = loansByMember.get(loan.MEMBER_NO);
      let remark = "";
      let targetNewContNo = null;
      let targetNewPrincipalBalance = null;

      // ตรวจสอบว่าสมาชิกมีหนี้คงเหลืออยู่หรือไม่
      if (memberLoans && memberLoans.length > 0) {
        // Logic เดิม: สำหรับกรณีที่สมาชิกมีหนี้คงเหลือ
        const principalBalance = principalBalanceMap.get(loan.LOANCONTRACT_NO);
        const isMatchingContract = memberLoans.some(
          (memberLoan) => memberLoan.LOANCONTRACT_NO === loan.LOANCONTRACT_NO
        );

        if (isMatchingContract) {
          // เงื่อนไข 1.1.1: เงินคืนตรงกับสัญญาเดิม (ที่อ้างอิงจาก CMSHLRONSLIPDET)
          if (principalBalance !== undefined && principalBalance <= 0) {
            const otherActiveLoans = findOtherActiveLoan(
              memberLoans,
              loan.LOANCONTRACT_NO
            );

            if (otherActiveLoans.length > 0) {
              const targetLoan = otherActiveLoans[0];
              const { loanUpdated, remarks } = await processPaymentLogic(
                connection,
                targetLoan,
                loan.INTEREST_RETURN
              );
              remark = remarks;
              targetNewContNo = targetLoan.LOANCONTRACT_NO;
              targetNewPrincipalBalance = loanUpdated.principal_balance;
            } else {
              remark = "โอนคืน-สมาชิกไม่มีหนี้";
            }
          } else {
            const targetLoan = { ...loan, PRINCIPAL_BALANCE: principalBalance };
            const { loanUpdated, remarks } = await processPaymentLogic(
              connection,
              targetLoan,
              loan.INTEREST_RETURN
            );
            remark = remarks;
            targetNewContNo = loan.LOANCONTRACT_NO;
            targetNewPrincipalBalance = loanUpdated.principal_balance;
          }
        } else {
          // เงื่อนไข 1.1.2: เงินคืนไม่ตรงกับสัญญาเดิม
          if (loan.NEWCONT_NO !== null && loan.NEWPRINCIPAL_BALANCE > 0) {
            const targetLoan = {
              ...loan,
              LOANCONTRACT_NO: loan.NEWCONT_NO,
              PRINCIPAL_BALANCE: loan.NEWPRINCIPAL_BALANCE,
            };
            const { loanUpdated, remarks } = await processPaymentLogic(
              connection,
              targetLoan,
              loan.INTEREST_RETURN
            );
            remark = remarks;
            targetNewContNo = loan.NEWCONT_NO;
            targetNewPrincipalBalance = loanUpdated.principal_balance;
          } else if (
            loan.NEWCONT_NO !== null &&
            loan.NEWPRINCIPAL_BALANCE <= 0
          ) {
            const otherActiveLoans = findOtherActiveLoan(
              memberLoans,
              loan.LOANCONTRACT_NO
            );

            if (otherActiveLoans.length > 0) {
              const targetLoan = otherActiveLoans[0];
              const { loanUpdated, remarks } = await processPaymentLogic(
                connection,
                targetLoan,
                loan.INTEREST_RETURN
              );
              remark = remarks;
              targetNewContNo = targetLoan.LOANCONTRACT_NO;
              targetNewPrincipalBalance = loanUpdated.principal_balance;
            } else {
              remark = `เงินคืนไม่ตรงกับสัญญาเดิม ${loan.LOANCONTRACT_NO} จะคืนในสัญญาใหม่ ${loan.NEWCONT_NO} หนี้คงเหลือเท่ากับ 0 แต่ไม่พบสัญญาอื่นของสมาชิก`;
            }
          } else if (loan.NEWCONT_NO === null) {
            const otherActiveLoans = findOtherActiveLoan(
              memberLoans,
              loan.LOANCONTRACT_NO
            );

            if (otherActiveLoans.length > 0) {
              const targetLoan = otherActiveLoans[0];
              const { loanUpdated, remarks } = await processPaymentLogic(
                connection,
                targetLoan,
                loan.INTEREST_RETURN
              );
              remark = remarks;
              targetNewContNo = targetLoan.LOANCONTRACT_NO;
              targetNewPrincipalBalance = loanUpdated.principal_balance;
            } else {
              remark = `เงินคืนไม่ตรงกับสัญญาเดิม ${loan.LOANCONTRACT_NO} และไม่พบสัญญาใหม่ ไม่พบสัญญาอื่นของสมาชิก`;
            }
          } else {
            remark = `เงินคืนไม่ตรงกับเงื่อนไขที่กำหนด ตรวจสอบ ${loan.LOANCONTRACT_NO} เพิ่มเติมในรายงาน`;
          }
        }
      } else {
        // แก้ไขตรงนี้: สำหรับกรณีที่สมาชิกไม่มีหนี้
        // ไม่ต้องทำ Transaction ใดๆ เพียงแค่เก็บข้อมูลเพื่อทำรายงาน
        remark = "โอนคืน-สมาชิกไม่มีหนี้";
        targetNewContNo = null;
        targetNewPrincipalBalance = null;
      }

      dataForReport.push({
        MEMBER_NO: loan.MEMBER_NO,
        LOANCONTRACT_NO: loan.LOANCONTRACT_NO,
        PRINCIPAL_BALANCE:
          principalBalanceMap.get(loan.LOANCONTRACT_NO) || null,
        INTEREST_RETURN: loan.INTEREST_RETURN,
        NEWCONT_NO: targetNewContNo,
        NEWPRINCIPAL_BALANCE: targetNewPrincipalBalance,
        REMARK: remark,
      });
    }

    await connection.commit();

    return {
      processedCount: dataForReport.length,
      message: `${dataForReport.length} records prepared for report.`,
      dataForReport: dataForReport,
    };
  } catch (err) {
    if (connection) {
      await connection.rollback();
    }
    throw new Error(`Processing failed: ${err.message}`);
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (err) {
        console.error(err);
      }
    }
  }
}

// ฟังก์ชันใหม่: ดึงข้อมูลเงินกู้ทั้งหมดที่มีหนี้คงเหลือ > 0
async function getPositivePrincipalLoans(connection) {
  const selectSql = `
        SELECT trim(MEMBER_NO) as MEMBER_NO, trim(LOANCONTRACT_NO) as LOANCONTRACT_NO, PRINCIPAL_BALANCE, PERIOD_PAYMENT 
        FROM LNCONTMASTER 
        WHERE PRINCIPAL_BALANCE > 0
    `;
  const result = await connection.execute(selectSql, [], {
    outFormat: oracledb.OUT_FORMAT_OBJECT,
  });
  return result.rows;
}
// ฟังก์ชันใหม่: ค้นหาสัญญาเงินกู้อื่นๆ ของสมาชิกที่มีหนี้คงเหลือ
// function findOtherActiveLoan(memberLoans, currentLoanContractNo) {
//     const otherLoans = memberLoans.filter(loan => loan.LOANCONTRACT_NO !== currentLoanContractNo);
//     if (otherLoans.length > 0) {
//         return otherLoans.map(loan => loan.LOANCONTRACT_NO).join(', ');
//     }
//     return '';
// }

// ปรับปรุงฟังก์ชัน: คืนค่า Array ของ Object ที่มีข้อมูลสัญญาเงินกู้อื่นๆ
function findOtherActiveLoan(memberLoans, currentLoanContractNo) {
  return memberLoans.filter(
    (loan) => loan.LOANCONTRACT_NO !== currentLoanContractNo
  );
}

async function processInterestReturnAndGenerateReport() {
  let connection;
  try {
    connection = await db.getConnection();

    const selectSql = `
            SELECT trim(sl.MEMBER_NO) as MEMBER_NO, trim(sd.LOANCONTRACT_NO) as LOANCONTRACT_NO , sd.INTEREST_RETURN as INTEREST_RETURN , sl.ref_newcontno as NEWCONT_NO , nlmn.principal_balance as NEWPRINCIPAL_BALANCE
            FROM cmshrlonslip sl
            JOIN cmshrlonslipdet sd on sl.slip_no = sd.slip_no
            JOIN mbmembmaster mb on sl.member_no = mb.member_no
            JOIN mbucfprename mpre on mb.prename_code = mpre.prename_code
            LEFT JOIN mbucfmembgroup mgrp on mb.membgroup_code = mgrp.membgroup_code
            LEFT JOIN lncontmaster nlmn on trim(nlmn.loancontract_no) = trim(sl.ref_newcontno)
            WHERE sl.slip_date between to_date('01/09/2025','dd/mm/yyyy') and to_date('30/09/2025','dd/mm/yyyy')
            AND sl.sliptype_code in ('PX','CLC')
            AND sd.slipitemtype_code = 'LON'
            AND sd.interest_return > 0
            AND sl.slip_status = 1
        `;
    const result = await connection.execute(selectSql, [], {
      outFormat: oracledb.OUT_FORMAT_OBJECT,
    });
    const loansToProcess = result.rows;

    if (loansToProcess.length === 0) {
      return {
        processedCount: 0,
        message: "No loans found with outstanding interest return.",
        status: false,
      };
    }

    const positivePrincipalLoans = await getPositivePrincipalLoans(connection);

    const loansByMember = new Map();
    for (const loan of positivePrincipalLoans) {
      if (!loansByMember.has(loan.MEMBER_NO)) {
        loansByMember.set(loan.MEMBER_NO, []);
      }
      loansByMember.get(loan.MEMBER_NO).push(loan);
    }

    const principalBalanceMap = new Map(
      positivePrincipalLoans.map((loan) => [
        loan.LOANCONTRACT_NO,
        loan.PRINCIPAL_BALANCE,
      ])
    );

    const dataForReport = [];

    for (const loan of loansToProcess) {
      const memberLoans = loansByMember.get(loan.MEMBER_NO);
      let remark = "";
      let targetNewContNo = null;
      let targetNewPrincipalBalance = null;

      if (memberLoans && memberLoans.length > 0) {
        const principalBalance = principalBalanceMap.get(loan.LOANCONTRACT_NO);
        const isMatchingContract = memberLoans.some(
          (memberLoan) => memberLoan.LOANCONTRACT_NO === loan.LOANCONTRACT_NO
        );

        if (isMatchingContract) {
          // เงื่อนไข 1.1.1: เงินคืนตรงกับสัญญาเดิม (ที่อ้างอิงจาก CMSHLRONSLIPDET)
          if (
            principalBalance !== undefined &&
            principalBalance <= loan.INTEREST_RETURN
          ) {
            const otherActiveLoans = findOtherActiveLoan(
              memberLoans,
              loan.LOANCONTRACT_NO
            );

            if (otherActiveLoans.length > 0) {
              remark = `เงินคืนตรงกับสัญญาเดิม ${loan.LOANCONTRACT_NO} แต่หนี้คงเหลือมีค่าเท่ากับ 0 จะคืนในสัญญาอื่นของสมาชิก ${otherActiveLoans[0].LOANCONTRACT_NO}`;
              targetNewContNo = otherActiveLoans[0].LOANCONTRACT_NO;
              targetNewPrincipalBalance = otherActiveLoans[0].PRINCIPAL_BALANCE;
            } else {
              remark = "โอนคืน-สมาชิกไม่มีหนี้";
            }
          } else {
            remark = `เงินคืนตรงกับสัญญาเดิม ${loan.LOANCONTRACT_NO}`;
            targetNewContNo = loan.LOANCONTRACT_NO;
            targetNewPrincipalBalance = principalBalance;
          }
        } else {
          // เงื่อนไข 1.1.2: เงินคืนไม่ตรงกับสัญญาเดิม (LOANCONTRACT_NO ใน cmshrlonslipdet ไม่มีหนี้คงเหลือ)
          if (loan.NEWCONT_NO !== null && loan.NEWPRINCIPAL_BALANCE > 0) {
            remark = `เงินคืนไม่ตรงกับสัญญาเดิม ${loan.LOANCONTRACT_NO} จะคืนในสัญญาใหม่ ${loan.NEWCONT_NO}`;
            targetNewContNo = loan.NEWCONT_NO;
            targetNewPrincipalBalance = loan.NEWPRINCIPAL_BALANCE;
          } else if (
            loan.NEWCONT_NO !== null &&
            loan.NEWPRINCIPAL_BALANCE === 0
          ) {
            const otherActiveLoans = findOtherActiveLoan(
              memberLoans,
              loan.LOANCONTRACT_NO
            );
            const otherContNo =
              otherActiveLoans.length > 0
                ? otherActiveLoans[0].LOANCONTRACT_NO
                : "ไม่พบ";
            remark = `เงินคืนไม่ตรงกับสัญญาเดิม ${loan.LOANCONTRACT_NO} จะคืนในสัญญาใหม่ ${loan.NEWCONT_NO} หนี้คงเหลือเท่ากับ 0 จะคืนในสัญญาอื่นของสมาชิก ${otherContNo}`;
            targetNewContNo =
              otherActiveLoans.length > 0
                ? otherActiveLoans[0].LOANCONTRACT_NO
                : null;
            targetNewPrincipalBalance =
              otherActiveLoans.length > 0
                ? otherActiveLoans[0].PRINCIPAL_BALANCE
                : null;
          } else if (loan.NEWCONT_NO === null) {
            const otherActiveLoans = findOtherActiveLoan(
              memberLoans,
              loan.LOANCONTRACT_NO
            );
            const otherContNo =
              otherActiveLoans.length > 0
                ? otherActiveLoans[0].LOANCONTRACT_NO
                : "ไม่พบ";
            remark = `เงินคืนไม่ตรงกับสัญญาเดิม ${loan.LOANCONTRACT_NO} จะคืนในสัญญาใหม่ ${loan.NEWCONT_NO} หนี้คงเหลือเท่ากับ 0 จะคืนในสัญญาอื่นของสมาชิก ${otherContNo}`;
            targetNewContNo =
              otherActiveLoans.length > 0
                ? otherActiveLoans[0].LOANCONTRACT_NO
                : null;
            targetNewPrincipalBalance =
              otherActiveLoans.length > 0
                ? otherActiveLoans[0].PRINCIPAL_BALANCE
                : null;
          } else {
            remark = `เงินคืนไม่ตรงกับเงื่อนไขที่กำหนด ตรวจสอบ ${loan.LOANCONTRACT_NO} เพิ่มเติมในรายงาน`;
          }
        }
      } else {
        remark = "โอนคืน-สมาชิกไม่มีหนี้";
      }

      dataForReport.push({
        MEMBER_NO: loan.MEMBER_NO,
        LOANCONTRACT_NO: loan.LOANCONTRACT_NO,
        PRINCIPAL_BALANCE:
          principalBalanceMap.get(loan.LOANCONTRACT_NO) || null,
        INTEREST_RETURN: loan.INTEREST_RETURN,
        NEWCONT_NO: targetNewContNo,
        NEWPRINCIPAL_BALANCE: targetNewPrincipalBalance,
        REMARK: remark,
      });
    }

    return {
      processedCount: dataForReport.length,
      message: `${dataForReport.length} records prepared for report.`,
      dataForReport: dataForReport,
    };
  } catch (err) {
    if (connection) {
      await connection.rollback();
    }
    throw new Error(`Processing failed: ${err.message}`);
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (err) {
        console.error(err);
      }
    }
  }
}

module.exports = {
  getAllLoanContracts,
  processInterestReturn,
  processInterestReturnAndGenerateReport,
};
