with
  source as (
  select
    *
  from
    {{ source('jrdb', 'raw_jrdb__ukc') }}
  where
    `データ年月日` != '99999999'
  ),

  source_deduped as (
  select
    *
  from (
    select
      *,
      -- See description of raw_jrdb__ukc for explanation of duplicates
      row_number() over (partition by `血統登録番号`, `データ年月日` order by `血統登録番号`, `データ年月日`) as rn
    from
      source
    )
  where
    rn = 1
  ),

  final as (
  select
    {{ dbt_utils.generate_surrogate_key(["`血統登録番号`", "`データ年月日`"]) }} as ukc_sk,
    nullif(trim(`血統登録番号`), '') as `血統登録番号`,
    nullif(trim(`馬名`), '') as `馬名`,
    nullif(trim(`性別コード`), '') as `性別コード`,
    nullif(trim(`毛色コード`), '') as `毛色コード`,
    -- See description for 馬記号コード for why we coalesce to '00'
    coalesce(nullif(trim(`馬記号コード`), ''), '00') as `馬記号コード`,
    nullif(trim(`血統情報_父馬名`), '') as `血統情報_父馬名`,
    nullif(trim(`血統情報_母馬名`), '') as `血統情報_母馬名`,
    nullif(trim(`血統情報_母父馬名`), '') as `血統情報_母父馬名`,
    to_date(nullif(trim(`生年月日`), ''), 'yyyyMMdd') as `生年月日`,
    cast(nullif(trim(`父馬生年`), '') as integer) as `父馬生年`,
    cast(nullif(trim(`母馬生年`), '') as integer) as `母馬生年`,
    cast(nullif(trim(`母父馬生年`), '') as integer) as `母父馬生年`,
    nullif(trim(`馬主名`), '') as `馬主名`,
    nullif(trim(`馬主会コード`), '') as `馬主会コード`,
    nullif(trim(`生産者名`), '') as `生産者名`,
    nullif(trim(`産地名`), '') as `産地名`,
    nullif(trim(`登録抹消フラグ`), '') as `登録抹消フラグ`,
    to_date(nullif(trim(`データ年月日`), ''), 'yyyyMMdd') as `データ年月日`,
    nullif(trim(`父系統コード`), '') as `父系統コード`,
    left(nullif(trim(`父系統コード`), ''), 2) as `父大系統コード`,
    nullif(trim(`母父系統コード`), '') as `母父系統コード`,
    left(nullif(trim(`母父系統コード`), ''), 2) as `母父大系統コード`,
    file_name as _file_name,
    sha256 as _sha256
  from
    source_deduped
)

select
  *
from
  final
