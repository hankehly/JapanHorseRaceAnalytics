-- Calculates the percent diff between the sum of the actual and pre-race place odds.
-- As of 2024-01-01 the average percent diff is 28.53% (real odds being higher).
SELECT
    SUM(`num_実績複勝オッズ`), -- 10767488.599999893
    SUM(`num_事前複勝オッズ`), -- 8079117.399999985
    -- 28.528969088651046
    (ABS(SUM(`num_実績複勝オッズ`) - SUM(`num_事前複勝オッズ`)) / ((SUM(`num_実績複勝オッズ`) + SUM(`num_事前複勝オッズ`)) / 2.0)) * 100 AS percent_diff
FROM
    jhra_intermediate.int_race_horses
