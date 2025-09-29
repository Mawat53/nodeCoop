//src/models/loancontractModel.js
const db = require("../config/database");
const oracledb = require("oracledb");

async function getAllLoanContracts() {
  let connection;
  try {
    connection = await db.getConnection();
    const result = await connection.execute(
      `SELECT MEMBER_NO, TRIM( LOANCONTRACT_NO ), PRINCIPAL_BALANCE, INTEREST_RETURN, PERIOD_PAYMENT, LASTCALINT_DATE FROM LNCONTMASTER WHERE PRINCIPAL_BALANCE > 0 AND MEMBER_NO = '023999'`
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
      `SELECT MEMBER_NO, TRIM(LOANCONTRACT_NO) as LOANCONTRACT_NO, PRINCIPAL_BALANCE, INTEREST_RETURN FROM LNCONTMASTER WHERE PRINCIPAL_BALANCE > 0 AND MEMBER_NO = '023999'`
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

function getThaiYear() {
  // ใน Node.js:
  const currentYearAD = new Date().getFullYear();
  return (currentYearAD + 543).toString().substring(2); // ได้ 68
}

/**
 * คืนค่าปีไทย 2 หลัก และเดือน 2 หลัก (YYMM)
 * เช่น 6809 สำหรับเดือนกันยายน พ.ศ. 2568
 */
function getThaiYearMonth() {
  const today = new Date();
  const currentYearAD = today.getFullYear();
  const currentMonth = today.getMonth() + 1; // เดือน 1-12

  // คำนวณปีไทย 2 หลัก (YY)
  const thaiYear = (currentYearAD + 543).toString().substring(2);

  // รูปแบบเดือน 2 หลัก (MM)
  const monthString = currentMonth.toString().padStart(2, "0");

  return thaiYear + monthString; // เช่น '6809'
}

// Function สมมติ: คำนวณดอกเบี้ยล่าสุด
// ในความเป็นจริงต้องเขียน Logic คำนวณตามเงื่อนไขทางธุรกิจ
async function calInterateLoan(connection, contractNo) {
  // contractNo ที่นี่จะเป็นสัญญาเป้าหมาย (Destination Contract) ที่ต้องการชำระ

  // SQL ถูกปรับปรุงเพื่อ:
  // 1. คำนวณจำนวนวัน (DAY_NUM): TRUNC(SYSDATE) - TRUNC(lastcalint_date)
  // 2. ใช้ NVL(..., 0) เพื่อจัดการกรณีที่ lastcalint_date เป็น NULL (ให้ถือเป็น 0 วัน) หรือ principal_balance เป็น NULL
  const interestSql = `
        SELECT 
            -- คำนวณจำนวนวัน: วันที่ปัจจุบัน - วันที่คิดดอกเบี้ยล่าสุด (TRUNC เพื่อเทียบแค่วันที่)
            NVL(TRUNC(SYSDATE) - TRUNC(lastcalint_date), 0) AS DAY_NUM,
            
            -- คำนวณดอกเบี้ยตามสูตร: (ยอดหนี้ * จำนวนวัน * อัตราดอกเบี้ย) / 365
            NVL(
                (NVL(principal_balance, 0) * NVL(TRUNC(SYSDATE) - TRUNC(lastcalint_date), 0) * (6 / 100)) / 365
            , 0) AS calculated_interest
        FROM 
            lncontmaster 
        WHERE 
            trim(loancontract_no) = :contractNo
    `;

    // 💡 วางตรงนี้เพื่อดูว่าสัญญาเป้าหมายเป็น 'undefined' หรือมีอักขระแปลกปลอมหรือไม่
    console.log("[DEBUG: CAL INT] Calculating interest for:", contractNo); 

  // กำหนดรูปแบบการส่งออกเป็น Object เพื่อการเข้าถึงข้อมูลที่ง่ายขึ้น
  const result = await connection.execute(
    interestSql,
    { contractNo },
    {
      outFormat: oracledb.OUT_FORMAT_OBJECT,
    }
  );

  // ป้องกัน NaN/Null และคืนค่าเป็นตัวเลข (Number) เสมอ
  const calculatedInterest =
    result.rows.length > 0 ? result.rows[0].CALCULATED_INTEREST : 0;

  return Number(calculatedInterest) || 0;
}

// Function สมมติ: Insert ข้อมูลลง lncontstatement
async function insertLoanStatement(
  connection,
  loanData,
  paymentType,
  amount,
  refNo
) {
  // **ป้องกัน NaN จาก amount ที่ส่งเข้ามา**
  const paymentAmount = Number(amount) || 0;

  // 1. ดึงค่า principal_balance ปัจจุบันของสัญญา
  const currentPrincipalSql = `SELECT principal_balance FROM lncontmaster WHERE TRIM(loancontract_no) = :contractNo`;
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
    paymentType === "principal"
      ? currentPrincipal - paymentAmount
      : currentPrincipal;

  const insertSql = `
        INSERT INTO lncontstatement (
            LOANCONTRACT_NO, SEQ_NO, LOANITEMTYPE_CODE, SLIP_DATE, OPERATE_DATE, REF_DOCNO, PERIOD,
            PRINCIPAL_PAYMENT, INTEREST_PAYMENT, PRINCIPAL_BALANCE, BFINTARREAR_AMT, INTEREST_PERIOD,
            INTEREST_ARREAR, INTEREST_RETURN, MONEYTYPE_CODE, ITEM_STATUS, ENTRY_ID, ENTRY_DATE, COOPBRANCH_ID
        ) VALUES (
        :contractNo, 
        (SELECT NVL(MAX(SEQ_NO), 0) + 1 FROM lncontstatement WHERE TRIM( LOANCONTRACT_NO ) = :contractNo),
        :loanItemTypeCode, :slipDate, :operateDate, :refNo, :period,
        :principalPayment, :interestPayment, :prinBal, :bfIntArrearAmt, :interestPeriod,
        :interestArrear, :interestReturn, :moneyTypeCode, :itemStatus, :entryId, SYSDATE, :coopBranchId
    )

    `;
  // console.log("loanData.LOANCONTRACT_NO",loanData.LOANCONTRACT_NO);
  const bindData = {
    contractNo: loanData.LOANCONTRACT_NO,
    slipDate: new Date(),
    operateDate: new Date(),
    refNo: refNo, // <--- ใช้ refNo ที่ส่งเข้ามา
    principalPayment: paymentType === "principal" ? paymentAmount : 0,
    interestPayment: paymentType === "interest" ? paymentAmount : 0,
    prinBal: newPrincipalBalance, // <--- ใช้ principal_balance ใหม่

    // **✅ ค่าที่ต้องตรวจสอบว่ามีอยู่ในตารางแม่:**
    loanItemTypeCode: "LRT", // 👈 ตรวจสอบว่า 'PX' มีอยู่ใน lnucfloanitemtype
    period: 0,
    bfIntArrearAmt: 0,
    interestPeriod: 0,
    interestArrear: 0,
    interestReturn: 0,
    moneyTypeCode: "TRN", // 👈 ตรวจสอบว่า 'LRT' มีอยู่ใน cmucfmoneytype
    itemStatus: 1,
    entryId: "iDontCare", // 👈 ตรวจสอบว่า 'AUTOPROC' มีอยู่ในตารางผู้ใช้งาน
    coopBranchId: "001", // 👈 ตรวจสอบว่า '001' มีอยู่ใน cmucfcoopbranch
  };
  // 💡 วางตรงนี้เพื่อตรวจสอบค่า Bind Data ทั้งหมด
    console.log("[DEBUG: INSERT BIND DATA] Contract:", loanData.LOANCONTRACT_NO);
    console.log("Bind Data:", JSON.stringify(bindData, null, 2)); 

  await connection.execute(insertSql, bindData);
}

/**
 * อัปเดตสัญญาต้นทาง: เคลียร์สถานะ INTEREST_RETURN = 0
 * @param {object} connection - OracleDB connection object
 * @param {string} contractNo - เลขที่สัญญาต้นทาง (Original Contract)
 */
async function updateLoanMasterOldContract(connection, contractNo) {
  const updateSql = `
        UPDATE lncontmaster 
        SET 
            INTEREST_RETURN = 0,
        WHERE trim(loancontract_no) = :contractNo
    `;
  await connection.execute(updateSql, { contractNo: contractNo });
}

/**
 * อัปเดตสัญญาปลายทาง: อัปเดตยอดหนี้คงเหลือและสถานะการชำระ
 * @param {object} connection - OracleDB connection object
 * @param {string} contractNo - เลขที่สัญญาปลายทาง (Destination Contract)
 * @param {number} principalPaid - จำนวนเงินต้นที่ชำระ
 * @param {number} interestPaid - จำนวนดอกเบี้ยที่ชำระ
 */
async function updateLoanMasterNewContract(
  connection,
  contractNo,
  principalPaid,
  interestPaid
) {
  // ป้องกัน NaN
  principalPaid = Number(principalPaid) || 0;
  interestPaid = Number(interestPaid) || 0;

  const updateSql = `
        UPDATE lncontmaster 
        SET 
            LAST_STM_NO = (SELECT NVL(MAX(SEQ_NO), 0) FROM lncontstatement WHERE TRIM(LOANCONTRACT_NO) = :contractNo),
            lastcalint_date = SYSDATE,
            principal_balance = GREATEST(0, principal_balance - :principalPaid),
            INTEREST_ARREAR = GREATEST(0, INTEREST_ARREAR - :interestPaid) 
        WHERE trim(loancontract_no) = :contractNo
    `;

  const bindData = {
    contractNo: contractNo,
    principalPaid: principalPaid,
    interestPaid: interestPaid,
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
    const refNoSql = `SELECT TRIM(last_documentno) AS LAST_DOCUMENTNO FROM cmshrlondoccontrol WHERE document_code = 'CMSLIPRECEIPT' FOR UPDATE`;
    const refNoResult = await connection.execute(refNoSql, [], {
      outFormat: oracledb.OUT_FORMAT_OBJECT,
    });
    const currentFullRefNo =
      refNoResult.rows.length > 0 ? refNoResult.rows[0].LAST_DOCUMENTNO : null;

    // 2. Generate the new document number.
    // 💡 ตรวจสอบความถูกต้องของ currentFullRefNo และกำหนดความยาวเริ่มต้น
    const defaultDocLength = 10; // สันนิษฐานความยาวมาตรฐาน เช่น YYMM + 6 หลัก
    const currentPrefix = getThaiYearMonth(); // เช่น '6809'
    const prefixLength = currentPrefix.length; // 4

    let nextDocNumber;

    // 🚨 ปรับปรุง 1: จัดการกรณี NULL หรือกรณีที่ Prefix ไม่ตรงกับเดือนปัจจุบัน
    if (
      !currentFullRefNo ||
      currentFullRefNo.substring(0, prefixLength) !== currentPrefix
    ) {
      // 💡 ถ้าเป็นครั้งแรกของเดือน หรือค่าเป็น NULL: รีเซ็ตตัวเลขลำดับเป็น 0
      const docNumberLength = currentFullRefNo
        ? currentFullRefNo.length - prefixLength
        : defaultDocLength - prefixLength; // ใช้ความยาวที่เหลือ

      nextDocNumber = (1) // เริ่มต้นที่ 1
        .toString()
        .padStart(docNumberLength, "0");
    } else {
      // กรณี Prefix ตรงกัน: ใช้ Logic การเพิ่มลำดับเดิม
      const currentDocNumberPart = currentFullRefNo.substring(prefixLength);

      // 🚨 แก้ไข: ใช้ parseInt(currentDocNumberPart) + 1
      nextDocNumber = (parseInt(currentDocNumberPart) + 1)
        .toString()
        .padStart(currentDocNumberPart.length, "0");
    }

    // 3. รวมเป็นหมายเลขเอกสารใหม่ (ใช้ในการบันทึก Statement และอัปเดต)
    const newRefNo = currentPrefix + nextDocNumber;
    const newLastDoc = nextDocNumber.toString();

    // 4. Call the original insertLoanStatement function with the new document number.
    await insertLoanStatement(
      connection,
      loanData,
      paymentType,
      amount,
      newRefNo.toString()
    );

    // 5. Update the document counter.
    const updateDocNoSql = `
            UPDATE cmshrlondoccontrol
            SET last_documentno = :newLastDoc
            WHERE document_code = 'CMSLIPRECEIPT'
        `;
    await connection.execute(updateDocNoSql, { newLastDoc });
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

  // ตรวจสอบและแปลงค่า Input (interestReturnAmount) เพื่อความปลอดภัย
  interestReturnAmount = Number(interestReturnAmount) || 0;

  // 1. Check the last interest calculation date.
  const checkDateSql = `SELECT TRUNC(lastcalint_date) AS LASTCALINT_DATE, LAST_STM_NO FROM lncontmaster WHERE trim(loancontract_no) = :contractNo`;
  const dateResult = await connection.execute(checkDateSql, {
    contractNo: loan.LOANCONTRACT_NO,
  });

  if (dateResult.rows.length === 0) {
    throw new Error(`Loan contract ${loan.LOANCONTRACT_NO} not found.`);
  }
  const { LASTCALINT_DATE: lastCalIntDate } = dateResult.rows[0];

  const today = new Date();
  today.setHours(0, 0, 0, 0);

  if (lastCalIntDate && lastCalIntDate.getTime() === today.getTime()) {
    // Case 1: Date is today. Pay principal with the return amount.
    principalPaid = interestReturnAmount;
    remarks = `เงินคืนตรงกับสัญญา ${loan.LOANCONTRACT_NO} และวันที่คิดดอกเบี้ยล่าสุดตรงกับวันนี้ จะนำเงินคืนไปชำระเงินต้นทั้งหมด`;

    // เมื่อคำนวณ principalPaid เสร็จแล้ว
    // ให้มั่นใจว่าค่าเหล่านี้เป็นตัวเลขที่ใช้ได้
    principalPaid = Number(principalPaid) || 0;

    // Use the new transaction function here.
    await createLoanStatementTransaction(
      connection,
      loan,
      "principal",
      principalPaid
    );
    // await updateLoanMaster(connection, loan, principalPaid, 0); // principalPaid, interestPaid
    // loanUpdated.principal_balance -= principalPaid;

    // ✅ เรียกใช้ฟังก์ชันอัปเดตสัญญาปลายทาง
    await updateLoanMasterNewContract(
      connection,
      loan.LOANCONTRACT_NO,
      principalPaid,
      0
    );

    loanUpdated.principal_balance -= principalPaid;
  } else {
    // Case 2: Date is not today. Calculate and pay interest first.
    const calculatedInterest = await calInterateLoan(
      connection,
      loan.LOANCONTRACT_NO
    );

    // เมื่อคำนวณ principalPaid และ interestPaid เสร็จแล้ว
    // ให้มั่นใจว่าค่าเหล่านี้เป็นตัวเลขที่ใช้ได้
    principalPaid = Number(principalPaid) || 0;
    interestPaid = Number(interestPaid) || 0;

    // Use the new transaction function to insert the interest payment.
    interestPaid = Math.min(interestReturnAmount, calculatedInterest);
    const remainingReturn = interestReturnAmount - interestPaid;

    if (remainingReturn > 0) {
      // If money is left, pay the principal.
      principalPaid = remainingReturn;
      remarks = `เงินคืนตรงกับสัญญา ${loan.LOANCONTRACT_NO} แต่วันที่คิดดอกเบี้ยไม่ตรง จะนำเงินคืนไปชำระดอกเบี้ย ${interestPaid} และที่เหลือ ${principalPaid} จะนำไปชำระเงินต้น`;

      // Use the new transaction function to insert the principal payment.
      await createLoanStatementTransaction(
        connection,
        loan,
        "principal",
        principalPaid
      );

      // await updateLoanMaster(connection, loan, principalPaid, interestPaid); // principalPaid, interestPaid

      // ✅ เรียกใช้ฟังก์ชันอัปเดตสัญญาปลายทาง
      await updateLoanMasterNewContract(
        connection,
        loan.LOANCONTRACT_NO,
        principalPaid,
        interestPaid
      );

      loanUpdated.principal_balance -= principalPaid;
    } else {
      // Not enough money to pay off the calculated interest.
      remarks = `เงินคืนตรงกับสัญญา ${loan.LOANCONTRACT_NO} แต่วันที่คิดดอกเบี้ยไม่ตรง และเงินคืน ${interestReturnAmount} ไม่เพียงพอสำหรับชำระดอกเบี้ยที่คำนวณได้ ${calculatedInterest}`;

      // Update the loan master record, paying what we can on interest.
      // await updateLoanMaster(connection, loan, 0, interestPaid); // principalPaid, interestPaid// ✅ เรียกใช้ฟังก์ชันอัปเดตสัญญาปลายทาง
      await updateLoanMasterNewContract(
        connection,
        loan.LOANCONTRACT_NO,
        0,
        interestPaid
      );
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
    const selectSql = `select member_no, TRIM(loancontract_no) as LOANCONTRACT_NO, interest_return, 'สป6802227' as newcont_no, 691000 as newprincipal_balance 
from lncontmaster where member_no = '023999' and TRIM(loancontract_no) = 'สป6700486' `;
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

    // ดึงข้อมูลเงินกู้ทั้งหมดที่มีหนี้คงเหลือ > 0
    // เพื่อใช้ในการตรวจสอบและประมวลผล
    // แทนที่จะดึงข้อมูลซ้ำๆ ในแต่ละรอบของ loop
    const positivePrincipalLoans = await getPositivePrincipalLoans(connection);

    // จัดกลุ่มข้อมูลเงินกู้ตาม MEMBER_NO เพื่อการค้นหาที่รวดเร็ว
    // แทนการใช้ filter ในแต่ละรอบของ loop
    // ซึ่งจะทำให้ประสิทธิภาพดีขึ้นมาก
    const loansByMember = new Map();
    for (const loan of positivePrincipalLoans) {
      if (!loansByMember.has(loan.MEMBER_NO)) {
        loansByMember.set(loan.MEMBER_NO, []);
      }
      loansByMember.get(loan.MEMBER_NO).push(loan);
    }

    // สร้างแผนที่ (Map) สำหรับการเข้าถึง PRINCIPAL_BALANCE ตาม LOANCONTRACT_NO
    // เพื่อให้การเข้าถึงข้อมูลรวดเร็วขึ้น
    // แทนการใช้ find หรือ filter ในแต่ละรอบของ loop
    // ซึ่งจะทำให้ประสิทธิภาพดีขึ้นมาก
    const principalBalanceMap = new Map(
      positivePrincipalLoans.map((loan) => [
        loan.LOANCONTRACT_NO,
        loan.PRINCIPAL_BALANCE,
      ])
    );

    // เตรียมข้อมูลสำหรับรายงาน
    const dataForReport = [];

    // Process each loan for interest return
    for (const loan of loansToProcess) {
      const originalContractNo = loan.LOANCONTRACT_NO; // 👈 เก็บสัญญาต้นทาง
      // ดึงข้อมูลเงินกู้ของสมาชิกจากแผนที่ (Map)
      // แทนการใช้ filter
      // ซึ่งจะทำให้ประสิทธิภาพดีขึ้นมาก
      const memberLoans = loansByMember.get(loan.MEMBER_NO);
      let remark = "";
      let targetNewContNo = null;
      let targetNewPrincipalBalance = null;

      // ตรวจสอบว่าสมาชิกมีหนี้คงเหลืออยู่หรือไม่
      if (memberLoans && memberLoans.length > 0) {
        // ดึง PRINCIPAL_BALANCE ของสัญญาเงินกู้ปัจจุบันจากแผนที่ (Map)
        const principalBalance = principalBalanceMap.get(loan.LOANCONTRACT_NO);

        // **⚠️ ต้องดึง PERIOD_PAYMENT ของสัญญาเป้าหมายเพื่อใช้ตรวจสอบ**
        // เนื่องจาก principalBalanceMap เก็บแค่ PRINCIPAL_BALANCE, เราต้องดึง Object สัญญาเต็มมา
        const currentLoanObject = memberLoans.find(
          (ml) => ml.LOANCONTRACT_NO === loan.LOANCONTRACT_NO
        );
        const periodPayment = currentLoanObject
          ? currentLoanObject.PERIOD_PAYMENT
          : 0; // ค่างวด
        const lastCalIntDate = currentLoanObject
          ? currentLoanObject.LASTCALINT_DATE
          : null; // วันที่คิดดอกเบี้ยล่าสุด

        // ตรวจสอบว่าสัญญาเงินกู้ใน slip มีหนี้คงเหลือหรือไม่
        const isMatchingContract = currentLoanObject !== undefined;

        // ตรวจสอบว่าสัญญาเงินกู้ใน slip มีหนี้คงเหลือหรือไม่
        // const isMatchingContract = memberLoans.some(
        //   (memberLoan) => memberLoan.LOANCONTRACT_NO === loan.LOANCONTRACT_NO
        // );

        // 🟢 Case ที่มีการเรียก processPaymentLogic
        const processAndRecordPayment = async (targetLoan) => {
          const { loanUpdated, remarks } = await processPaymentLogic(
            connection,
            targetLoan,
            loan.INTEREST_RETURN
          );

          // ✅ NEW: อัปเดตสัญญาต้นทางเพื่อเคลียร์สถานะ INTEREST_RETURN
          await updateLoanMasterOldContract(connection, originalContractNo);

          return { loanUpdated, remarks };
        };

        if (isMatchingContract) {
          // เงื่อนไข 1.1.1: เงินคืนตรงกับสัญญาเดิม
          if (
            (principalBalance !== undefined && principalBalance <= 0) ||
            principalBalance === periodPayment
          ) {
            // กรณีที่หนี้คงเหลือมีค่าเท่ากับ 0 หรือเท่ากับค่างวด (ต้องหาสัญญาอื่น)
            const otherActiveLoans = findOtherActiveLoan(
              memberLoans,
              loan.LOANCONTRACT_NO
            );

            if (otherActiveLoans.length > 0) {
              const targetLoan = otherActiveLoans[0];
              // 🚨 เพิ่มการตรวจสอบความปลอดภัย 1
              if (!targetLoan || !targetLoan.LOANCONTRACT_NO) {
                console.log(
                  "🚨 Security Check Failed: targetLoan or LOANCONTRACT_NO is invalid",
                  targetLoan
                );
                remark = "พบสัญญาอื่น แต่ไม่มีหมายเลขสัญญาที่ถูกต้อง";
              } else {
                const { loanUpdated, remarks } = await processAndRecordPayment(
                  targetLoan
                );
                remark = remarks;
                targetNewContNo = targetLoan.LOANCONTRACT_NO;
                targetNewPrincipalBalance = loanUpdated.principal_balance;
              }
            } else {
              remark = "โอนคืน-สมาชิกไม่มีหนี้";
            }
          } else {
            // กรณีใช้สัญญาเดิมชำระ (Original = Destination)
            const targetLoan = { ...loan, PRINCIPAL_BALANCE: principalBalance };
            const { loanUpdated, remarks } = await processAndRecordPayment(
              targetLoan
            );
            remark = remarks;
            targetNewContNo = loan.LOANCONTRACT_NO;
            targetNewPrincipalBalance = loanUpdated.principal_balance;
          }
        } else {
          // เงื่อนไข 1.1.2: เงินคืนไม่ตรงกับสัญญาเดิม ให้หาสัญญาใหม่ที่จะคืนเงิน
          if (loan.NEWCONT_NO !== null && loan.NEWPRINCIPAL_BALANCE > 0) {
            const targetLoan = {
              ...loan,
              LOANCONTRACT_NO: loan.NEWCONT_NO,
              PRINCIPAL_BALANCE: loan.NEWPRINCIPAL_BALANCE,
            };
            const { loanUpdated, remarks } = await processAndRecordPayment(
              targetLoan
            );
            remark = remarks;
            targetNewContNo = loan.NEWCONT_NO;
            targetNewPrincipalBalance = loanUpdated.principal_balance;
          } else if (
            loan.NEWCONT_NO !== null &&
            loan.NEWPRINCIPAL_BALANCE <= 0
          ) {
            // หาจากสัญญาอื่น
            const otherActiveLoans = findOtherActiveLoan(
              memberLoans,
              loan.LOANCONTRACT_NO
            );

            if (otherActiveLoans.length > 0) {
              const targetLoan = otherActiveLoans[0];
              // 🚨 เพิ่มการตรวจสอบความปลอดภัย 2
              if (!targetLoan || !targetLoan.LOANCONTRACT_NO) {
                remark = "พบสัญญาอื่น แต่ไม่มีหมายเลขสัญญาที่ถูกต้อง";
              } else {
                const { loanUpdated, remarks } = await processAndRecordPayment(
                  targetLoan
                );
                remark = remarks;
                targetNewContNo = targetLoan.LOANCONTRACT_NO;
                targetNewPrincipalBalance = loanUpdated.principal_balance;
              }
            } else {
              remark = `เงินคืนไม่ตรงกับสัญญาเดิม ${loan.LOANCONTRACT_NO} จะคืนในสัญญาใหม่ ${loan.NEWCONT_NO} หนี้คงเหลือเท่ากับ 0 แต่ไม่พบสัญญาอื่นของสมาชิก`;
            }
          } else if (loan.NEWCONT_NO === null) {
            // หาจากสัญญาอื่น (เนื่องจากไม่มี NEWCONT_NO)
            const otherActiveLoans = findOtherActiveLoan(
              memberLoans,
              loan.LOANCONTRACT_NO
            );

            if (otherActiveLoans.length > 0) {
              const targetLoan = otherActiveLoans[0];
              // 🚨 เพิ่มการตรวจสอบความปลอดภัย 3
              if (!targetLoan || !targetLoan.LOANCONTRACT_NO) {
                remark = "พบสัญญาอื่น แต่ไม่มีหมายเลขสัญญาที่ถูกต้อง";
              } else {
                const { loanUpdated, remarks } = await processAndRecordPayment(
                  targetLoan
                );
                remark = remarks;
                targetNewContNo = targetLoan.LOANCONTRACT_NO;
                targetNewPrincipalBalance = loanUpdated.principal_balance;
              }
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
        SELECT trim(MEMBER_NO) as MEMBER_NO, TRIM(LOANCONTRACT_NO) as LOANCONTRACT_NO, PRINCIPAL_BALANCE, PERIOD_PAYMENT 
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
