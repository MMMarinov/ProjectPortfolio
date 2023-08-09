/* 

Sales Incurment Query 

This query was needed when a Transaction table was depreciated and the data needed from it could not be transfered or retained.
The query uses a set of tables and manipulates the information in such a way that the data still required from the depreciated table 
can be gathered and presented to the key stakeholders. 

Key Highlights: 
- Extracting key information from several database tables
- Transforming information that served one intended purpose and bending that data structure to work in favour of the business problem
- Loading the final result into a report, with an easy to extract output

*/
WITH LineOfBussFilter AS -- Initial Filter meant to remove any records outside of scope
(
    SELECT  DISTINCT REF_NUM
    FROM
    (
        SELECT  REF_NUM
        FROM GENERAL_TBL
        WHERE 1 = 1
        AND ( LINE_OF_BUSINESS NOT IN ('AA', 'BB') ) 
        UNION ALL
        SELECT  REF_NUM
        FROM GENERAL_TBL_HIST
        WHERE 1 = 1
        AND ( LINE_OF_BUSINESS NOT IN ('AA', 'BB') ) 
    ) CT
), General AS -- General information gathering from the main general table and history table
(
    SELECT  ISNULL(GTH.REF_NUM,GT.REF_NUM) REF_NUM,
            ISNULL(GTH.ENTITY,GT.ENTITY) ENTITY,
            ISNULL(GTH.ACTION_DT,GT.ACTION_DT) ACTION_DT,
            ISNULL(GTH.COMPANY_NUM,GT.COMPANY_NUM) COMPANY_NUM,
            ISNULL(GTH.CONTRACT_NUM,GT.CONTRACT_NUM) CONTRACT_NUM,
            ISNULL(GTH.HISTORY_NUM,GT.HISTORY_NUM) HISTORY_NUM,
            ISNULL(GTH.REF_CREATED_DT,GT.REF_CREATED_DT) REF_CREATED_DT
    FROM GENERAL_TBL_HIST GTH
    FULL JOIN GENERAL_TBL GT
    ON GTH.REF_NUM = GT.REF_NUM AND GTH.HISTORY_NUM = GT.HISTORY_NUM
    LEFT JOIN LineOfBussFilter LBF
    ON ISNULL(GTH.REF_NUM, GT.REF_NUM) = LBF.REF_NUM
    WHERE 1 = 1
    AND LBF.REF_NUM IS NULL 
), Coverage AS -- Similar to General, gathering information along with preparing the Sales Incurment amount by filtering out unneeded statuses
(
    SELECT  ISNULL(CT.REF_NUM,CTH.REF_NUM) REF_NUM,
            ISNULL(CT.HISTORY_NUM,CTH.HISTORY_NUM) HISTORY_NUM,
            ISNULL(CT.ENTITY,CTH.ENTITY) ENTITY,
            ISNULL(CT.STATUS,CTH.STATUS) STATUS,
            CASE WHEN ISNULL(CT.STATUS,CTH.STATUS) IN (9) THEN ISNULL(SUM(ISNULL(CT.AMOUNT,CTH.AMOUNT)),0)
                 WHEN ISNULL(CT.STATUS,CTH.STATUS) NOT IN (1,6,9,11,14) THEN 0  ELSE SUM(ISNULL(CT.AMOUNT,CTH.AMOUNT)) END AS AMOUNT
    FROM COVERAGE_TBL_HIST CTH
    FULL JOIN COVERAGE_TBL CT
    ON CTH.REF_NUM = CT.REF_NUM AND CTH.HISTORY_NUM = CT.HISTORY_NUM
    LEFT JOIN LineOfBussFilter LBF
    ON ISNULL(CTH.REF_NUM, CT.REF_NUM) = LBF.REF_NUM
    WHERE 1 = 1
    AND LBF.REF_NUM IS NULL
    GROUP BY  ISNULL(CT.REF_NUM,CTH.REF_NUM),
              ISNULL(CT.HISTORY_NUM,CTH.HISTORY_NUM),
              ISNULL(CT.ENTITY,CTH.ENTITY),
              ISNULL(CT.STATUS,CTH.STATUS)
), TableCombining AS -- Blend the General and Coverage table to ensure that no gaps are missing between the main data points
(
    SELECT  ISNULL(CT.REF_NUM,GT.REF_NUM) REF_NUM,
            E.Name ENTITY,
            ISNULL(CT.PARENT_COMP_NUM,CT.COMP_NUM) PARENT_COMPANY_NUMBER,
            ISNULL(CT.PARENT_COMP_NAME,CT.COMP_NAME) PARENT_COMPANY_NAME,
            ISNULL(GT.COMPANY_NUM,GT1.COMPANY_NUM) COMPANY_NUM,
            ISNULL(GT.ACTION_DT,GT1.ACTION_DT) ACTION_DT,
            ISNULL(GT.REF_CREATED_DT,GT1.REF_CREATED_DT) REF_CREATED_DT,
            CT.COMP_NAME COMPANY,
            ISNULL(CT.HISTORY_NUM,GT.HISTORY_NUM) HISTORY_NUM,
            GT1.CONTRACT_NUM,
            L.Name LINE_OF_BUSINESS,
            ACT.TRANSACTION_DT,
            CT.AMOUNT AMOUNT
    FROM General GT
    FULL JOIN Coverage CT
    ON GT.REF_NUM = CT.REF_NUM AND GT.HISTORY_NUM = CT.HISTORY_NUM
    LEFT JOIN ACTIVITY_TBL ACT
    ON ISNULL(CT.REF_NUM, GT.REF_NUM) = ACT.REF_NUM AND ISNULL(CT.HISTORY_NUM, GT.HISTORY_NUM) = ACT.CLAIM_HIST_NUM
    LEFT JOIN GENERAL_TBL GT1
    ON ISNULL(CT.REF_NUM, GT.REF_NUM) = GT1.REF_NUM
    LEFT JOIN COMPANY_TBL CT
    ON CT.COMP_NUM = GT1.COMPANY_NUM
    LEFT JOIN LOB_TBL AS L
    ON L.Code = GT1.LINE_OF_BUSINESS
    LEFT JOIN ENTITY_TBL AS E
    ON E.Code = ISNULL(CT.ENTITY, GT.ENTITY)
    WHERE 1 = 1
    AND AMOUNT IS NOT NULL 
), DeltaCalculation AS -- Begin looking through the history of every change ocurrance found in the history tables AND tracking the $ Diff between each transaction. Delta Calc uses Lag to identify the previous $ record and subtracts
(
    SELECT  *,
            ISNULL(AMOUNT,0) - LAG(AMOUNT) OVER (PARTITION BY REF_NUM ORDER BY TC.TRANSACTION_DT) AS DELTACALC,
            LAG(TRANSACTION_DT) OVER (PARTITION BY REF_NUM ORDER BY TRANSACTION_DT)               AS DELTADATE
    FROM TableCombining TC
), RowNumberAssignment AS -- Assigning an incremental count that resets when a $ diff is found for each new ref_num. A record with no $ change from the previous will continue the count
(
    SELECT  *,
            ROW_NUMBER() OVER (PARTITION BY REF_NUM,DELTACALC,MONTH(TRANSACTION_DT) ORDER BY DC.TRANSACTION_DT DESC) RNdsc
    FROM DeltaCalculation DC
), FinalPreperations AS -- Filtering for only records where a change has occured. This is tracked as a transaction incurment or reversal.
(
SELECT  *,
        ISNULL(AMOUNT - LAG(AMOUNT) OVER (PARTITION BY REF_NUM ORDER BY RNA.TRANSACTION_DT),AMOUNT) Amount
FROM RowNumberAssignment RNA
WHERE RNdsc = 1
), Final AS -- Prepares the final output with the columns in a specific order and with the transaction amount calculated
(
    SELECT  F.ENTITY,
            F.PARENT_COMPANY_NAME,
            F.PARENT_COMPANY_NUMBER,
            F.COMPANY,
            F.COMPANY_NUM,
            F.CONTRACT_NUM,
            F.REF_NUM,
            F.LINE_OF_BUSINESS,
            F.REF_CREATED_DT,
            SUM(AMOUNT) AMOUNT,
            YEAR(F.TRANSACTION_DT) [YEAR-DATE],
            FORMAT(F.TRANSACTION_DT,'MMM') [MONTH-DATE],
            DAY(F.TRANSACTION_DT) [DAY-DATE],
            CAST(F.TRANSACTION_DT AS DATE) [DATE]
    FROM FinalPreperations F
    GROUP BY  ENTITY,
              PARENT_COMPANY_NAME,
              PARENT_COMPANY_NUMBER,
              COMPANY,
              COMPANY_NUM,
              CONTRACT_NUM,
              F.REF_NUM,
              LINE_OF_BUSINESS,
              TRANSACTION_DT,
              ACTION_DT,
              REF_CREATED_DT,
              DELTADATE,
              AMOUNT,
              L.[LargeR]
    HAVING SUM(AMOUNT) <> 0
)
SELECT  *
FROM Final
