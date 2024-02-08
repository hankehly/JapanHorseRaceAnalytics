-- This should be a test
select
	`競走成績キー_年月日`,
	`競走成績キー_血統登録番号`,
	count(*)
from
    {{ ref('stg_jrdb__sed') }}
group by
	`競走成績キー_年月日`,
	`競走成績キー_血統登録番号`
having
	count(*) > 1
order by
	`競走成績キー_年月日`,
	`競走成績キー_血統登録番号`
