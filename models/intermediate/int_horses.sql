{{ config(materialized='table') }}
with
  -- Get the latest data for each horse (scd type 2)
  ukc_latest AS (
  select
    *
  from
    {{ ref('stg_jrdb__ukc') }}
  where
    (血統登録番号, データ年月日) in (select 血統登録番号, MAX(データ年月日) from {{ ref('stg_jrdb__ukc') }} group by 血統登録番号)
  ),

  good_finish as (
  select
    "競走成績キー_血統登録番号" as "血統登録番号",
    sum(
      case
        when "ＪＲＤＢデータ_馬場差" <= -10 and "馬成績_着順" <= 3 then 1
        else 0
      end
    ) > 0 "瞬発戦好走馬",
    sum(
      case
  	    when "ＪＲＤＢデータ_馬場差" >= 10 and "馬成績_着順" <= 3 then 1
  	    else 0
      end
    ) > 0 "消耗戦好走馬"
  from
    {{ ref('stg_jrdb__sed') }}
  where
    馬成績_異常区分 = '0'
  group by
    "血統登録番号"
  ),

  final as (
  select
    ukc."血統登録番号",
    case
      when ukc."性別コード" = '1' then '牡'
      when ukc."性別コード" = '2' then '牝'
      else 'セン'
    end "性別",
    "瞬発戦好走馬",
    "消耗戦好走馬"
  from
    ukc_latest as ukc
  left join
    good_finish
  on
    ukc."血統登録番号" = good_finish."血統登録番号"
  )

select * from final
