CREATE OR REPLACE TABLE  `data-engineer5125.SI_analysys.SI_raw_aferTransaction`AS
(WITH `temp1` AS #temp1はカラム名を変更&YYYY年MMorM年DDになっているもののみ抽出(deteでpがついているのもを除去)
    (SELECT 
    ____________ AS item
    ,_____________________ AS industry
    ,______ AS area, 
    ,CASE WHEN ____________________________________ LIKE '%p' 
    THEN REPLACE(____________________________________,'  p','') 
    ELSE ____________________________________ END AS date 
    ,unit
    ,value
    FROM `data-engineer5125.SI_analysys.SI_raw`
    WHERE REGEXP_CONTAINS( ____________________________________,'.*年.月|.*年..月')
    )
    ,`temp2` AS  #temp2は年を-0or-に変更
    (SELECT 
    CASE WHEN date LIKE '%p' 
    THEN REPLACE(date,'  p','') 
    WHEN date LIKE '%年_月' 
    THEN REPLACE(date,'年','-0')
    WHEN date LIKE '%年__月' 
    THEN REPLACE(date,'年','-') 
    ELSE date END AS date
    ,item
    ,industry
    ,area
    ,value
    ,unit
    FROM `temp1`
    )

#temp2の月を-01に変更して抽出
SELECT 
    REPLACE(date,'月','-01') AS date
    ,item
    ,industry
    ,area
    ,value
    ,unit 
FROM `temp2`
ORDER BY industry ,date ASC)