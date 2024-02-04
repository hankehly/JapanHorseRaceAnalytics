with
  source as (
  select
    *,
    row_number() over (partition by `station_name`, `年月日時` order by `download_timestamp` desc) as row_number
  from
    {{ source('jma', 'raw_jma__weather_hourly') }}
  ),
  final as (
  select
    weather_hourly_sk,
    to_timestamp(`download_timestamp`, 'yyyy-MM-dd HH:mm:ssXXX') AS `download_timestamp`,
    nullif(`station_name`, '') as `station_name`,
    to_timestamp(`年月日時`, 'yyyy-MM-dd HH:mm:ss') `年月日時`,
    cast(nullif(`気温`, '') as numeric) as `気温`,
    nullif(`気温_品質情報`, '') as `気温_品質情報`,
    nullif(`気温_均質番号`, '') as `気温_均質番号`,
    cast(nullif(`降水量`, '') as numeric) as `降水量`,
    nullif(`降水量_品質情報`, '') as `降水量_品質情報`,
    nullif(`降水量_均質番号`, '') as `降水量_均質番号`,
    cast(nullif(`降雪`, '') as numeric) as `降雪`,
    nullif(`降雪_品質情報`, '') as `降雪_品質情報`,
    nullif(`降雪_均質番号`, '') as `降雪_均質番号`,
    cast(nullif(`積雪`, '') as numeric) as `積雪`,
    nullif(`積雪_品質情報`, '') as `積雪_品質情報`,
    nullif(`積雪_均質番号`, '') as `積雪_均質番号`,
    nullif(`日照時間`, '') as `日照時間`,
    nullif(`日照時間_品質情報`, '') as `日照時間_品質情報`,
    nullif(`日照時間_均質番号`, '') as `日照時間_均質番号`,
    cast(nullif(`風速`, '') as numeric) as `風速`,
    nullif(`風速_品質情報`, '') as `風速_品質情報`,
    -- there are about 10-20 rows containing a trailing ')'
    trim(trailing ')' from nullif(`風速_風向`, '')) as `風速_風向`,
    nullif(`風速_風向_品質情報`, '') as `風速_風向_品質情報`,
    nullif(`風速_均質番号`, '') as `風速_均質番号`,
    cast(nullif(`日射量`, '') as numeric) as `日射量`,
    nullif(`日射量_品質情報`, '') as `日射量_品質情報`,
    nullif(`日射量_均質番号`, '') as `日射量_均質番号`,
    cast(nullif(`現地気圧`, '') as numeric) as `現地気圧`,
    nullif(`現地気圧_品質情報`, '') as `現地気圧_品質情報`,
    nullif(`現地気圧_均質番号`, '') as `現地気圧_均質番号`,
    cast(nullif(`海面気圧`, '') as numeric) as `海面気圧`,
    nullif(`海面気圧_品質情報`, '') as `海面気圧_品質情報`,
    nullif(`海面気圧_均質番号`, '') as `海面気圧_均質番号`,
    cast(nullif(`相対湿度`, '') as integer) as `相対湿度`,
    nullif(`相対湿度_品質情報`, '') as `相対湿度_品質情報`,
    nullif(`相対湿度_均質番号`, '') as `相対湿度_均質番号`,
    cast(nullif(`蒸気圧`, '') as numeric) as `蒸気圧`,
    nullif(`蒸気圧_品質情報`, '') as `蒸気圧_品質情報`,
    nullif(`蒸気圧_均質番号`, '') as `蒸気圧_均質番号`,
    cast(nullif(`露点温度`, '') as numeric) as `露点温度`,
    nullif(`露点温度_品質情報`, '') as `露点温度_品質情報`,
    nullif(`露点温度_均質番号`, '') as `露点温度_均質番号`,
    -- values were turning into floats which caused reference error..
    cast(cast(nullif(`天気`, '') as integer) as string) as `天気`,
    nullif(`天気_品質情報`, '') as `天気_品質情報`,
    nullif(`天気_均質番号`, '') as `天気_均質番号`,
    nullif(`雲量`, '') as `雲量`,
    nullif(`雲量_品質情報`, '') as `雲量_品質情報`,
    nullif(`雲量_均質番号`, '') as `雲量_均質番号`,
    cast(nullif(`視程`, '') as numeric) as `視程`,
    nullif(`視程_品質情報`, '') as `視程_品質情報`,
    nullif(`視程_均質番号`, '') as `視程_均質番号`
  from
    source
  where
    row_number = 1
)

select
  *
from
  final
