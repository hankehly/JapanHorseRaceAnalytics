with
  good_finishes as (
  select
    競走成績キー_血統登録番号 as "血統登録番号",
    case
      when "ＪＲＤＢデータ_馬場差" <= -10 and "馬成績_着順" <= 3 then 1
      else 0
    end "瞬発戦好走",
    case
  	  when "ＪＲＤＢデータ_馬場差" >= 10 and "馬成績_着順" <= 3 then 1
  	  else 0
    end "消耗戦好走"
  from
    {{ ref('stg_jrdb__sed') }}
  where
    馬成績_異常区分 = '0'
  ),

  final as (
  select
    "血統登録番号",
    sum("瞬発戦好走") > 0 "瞬発戦好走馬",
    sum("消耗戦好走") > 0 "消耗戦好走馬"
  from
    good_finishes
  group by
    "血統登録番号"
  )

select * from final
