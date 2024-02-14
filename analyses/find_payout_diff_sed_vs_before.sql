-- Finds records where SED payouts do not match the payouts in the intermediate tables
-- There are none in the current data (as of 2024-02-15)
SELECT
    sed.`レースキー`,
    sed.`馬番`,
	sed.`払戻データ_複勝` as `sed_複勝払戻金`,
	place_payouts.`払戻金` as `hjc_複勝払戻金`,
	sed.`払戻データ_単勝` as `sed_単勝払戻金`,
	win_payouts.`払戻金` as `hjc_単勝払戻金`
FROM
    {{ ref('stg_jrdb__sed') }} sed
LEFT JOIN
    {{ ref('int_place_payouts') }} place_payouts
ON
    sed.`レースキー` = place_payouts.`レースキー`
    AND sed.`馬番` = place_payouts.`馬番`
LEFT JOIN
    {{ ref('int_win_payouts') }} win_payouts
ON
    sed.`レースキー` = win_payouts.`レースキー`
    AND sed.`馬番` = win_payouts.`馬番`
WHERE 
    sed.`払戻データ_複勝` != place_payouts.`払戻金`
    OR sed.`払戻データ_単勝` != win_payouts.`払戻金`
LIMIT
    1000
