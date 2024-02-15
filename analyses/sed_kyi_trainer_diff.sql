SELECT
    sed.`レースキー`,
    sed.`馬番`,
    sed.`調教師コード` as `sed_調教師コード`,
    kyi.`調教師コード` as `kyi_調教師コード`
FROM
	jhra_staging.stg_jrdb__sed sed
LEFT JOIN
    jhra_staging.stg_jrdb__tyb tyb
ON
    sed.`レースキー` = tyb.`レースキー`
    AND sed.`馬番` = tyb.`馬番`
LEFT JOIN
    jhra_staging.stg_jrdb__kyi kyi
ON
    sed.`レースキー` = kyi.`レースキー`
    AND sed.`馬番` = kyi.`馬番`
WHERE 
    sed.`調教師コード` != kyi.`調教師コード`
