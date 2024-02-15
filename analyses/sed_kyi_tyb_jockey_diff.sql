SELECT
    sed.`レースキー`,
    sed.`馬番`,
    sed.`騎手コード` as `sed_騎手コード`,
    sed.`馬成績_騎手名` as `sed_騎手名`,
    tyb.`騎手コード` as `tyb_騎手コード`,
    tyb.`騎手名` as `tyb_騎手名`,
    kyi.`騎手コード` as `kyi_騎手コード`
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
    sed.`騎手コード` != tyb.`騎手コード`
    OR sed.`騎手コード` != kyi.`騎手コード`
