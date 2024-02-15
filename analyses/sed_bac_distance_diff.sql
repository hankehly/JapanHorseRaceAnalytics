SELECT DISTINCT
    sed.`レースキー`,
    sed.`レース条件_距離` as `sed_レース条件_距離`,
    bac.`レース条件_距離` as `bac_レース条件_距離`
FROM
	jhra_staging.stg_jrdb__sed sed
LEFT JOIN
    jhra_staging.stg_jrdb__bac bac
ON
    sed.`レースキー` = bac.`レースキー`
WHERE 
    sed.`レース条件_距離` != bac.`レース条件_距離`
