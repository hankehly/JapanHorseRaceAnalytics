with source as (
      select * from {{ source('jrdb', 'raw_jrdb__skb') }}
),
final as (
    select
        skb_sk,
        concat(
            nullif(`レースキー_場コード`, ''),
            nullif(`レースキー_年`, ''),
            nullif(`レースキー_回`, ''),
            nullif(`レースキー_日`, ''),
            nullif(`レースキー_Ｒ`, ''),
            nullif(`馬番`, '')
        ) as skb_bk,
        concat(
            nullif(`レースキー_場コード`, ''),
            nullif(`レースキー_年`, ''),
            nullif(`レースキー_回`, ''),
            nullif(`レースキー_日`, ''),
            nullif(`レースキー_Ｒ`, '')
        ) as `レースキー`,
        concat(
            nullif(`レースキー_場コード`, ''),
            nullif(`レースキー_年`, ''),
            nullif(`レースキー_回`, ''),
            nullif(`レースキー_日`, '')
        ) as `開催キー`,
        nullif(`レースキー_場コード`, '') as `レースキー_場コード`,
        nullif(`レースキー_年`, '') as `レースキー_年`,
        nullif(`レースキー_回`, '') as `レースキー_回`,
        nullif(`レースキー_日`, '') as `レースキー_日`,
        nullif(`レースキー_Ｒ`, '') as `レースキー_Ｒ`,
        nullif(`馬番`, '') as `馬番`,
        nullif(`競走成績キー_血統登録番号`, '') as `競走成績キー_血統登録番号`,
        to_date(nullif(`競走成績キー_年月日`, ''), 'yyyyMMdd') as `競走成績キー_年月日`,
        `特記コード`,
        `馬具コード`,
        `脚元コード_総合`,
        `脚元コード_左前`,
        `脚元コード_右前`,
        `脚元コード_左後`,
        `脚元コード_右後`,
        nullif(`パドックコメント`, '') as `パドックコメント`,
        nullif(`脚元コメント`, '') as `脚元コメント`,
        nullif(`馬具(その他)コメント`, '') as `馬具(その他)コメント`,
        nullif(`レースコメント`, '') as `レースコメント`,
        nullif(`分析用データ_ハミ`, '') as `分析用データ_ハミ`,
        nullif(`分析用データ_バンテージ`, '') as `分析用データ_バンテージ`,
        nullif(`分析用データ_蹄鉄`, '') as `分析用データ_蹄鉄`,
        nullif(`分析用データ_蹄状態`, '') as `分析用データ_蹄状態`,
        nullif(`分析用データ_ソエ`, '') as `分析用データ_ソエ`,
        nullif(`分析用データ_骨瘤`, '') as `分析用データ_骨瘤`
    from source
)
select * from final
  