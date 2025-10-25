with
    source as (
    select
        *
    from
        {{ source('jrdb', 'raw_jrdb__srb') }}
    ),

    final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            "`レースキー_場コード`",
            "`レースキー_年`",
            "`レースキー_回`",
            "`レースキー_日`",
            "`レースキー_Ｒ`"
        ]) }} as srb_sk,
        concat(
            `レースキー_場コード`,
            `レースキー_年`,
            `レースキー_回`,
            `レースキー_日`,
            `レースキー_Ｒ`
        ) as `レースキー`,
        nullif(trim(`レースキー_場コード`), '') as `レースキー_場コード`,
        nullif(trim(`レースキー_年`), '') as `レースキー_年`,
        nullif(trim(`レースキー_回`), '') as `レースキー_回`,
        nullif(trim(`レースキー_日`), '') as `レースキー_日`,
        nullif(trim(`レースキー_Ｒ`), '') as `レースキー_Ｒ`,
        `ハロンタイム` as `ハロンタイム`,
        nullif(trim(`コーナー位置取り_１コーナー`), '') as `コーナー位置取り_１コーナー`,
        nullif(trim(`コーナー位置取り_２コーナー`), '') as `コーナー位置取り_２コーナー`,
        nullif(trim(`コーナー位置取り_３コーナー`), '') as `コーナー位置取り_３コーナー`,
        nullif(trim(`コーナー位置取り_４コーナー`), '') as `コーナー位置取り_４コーナー`,
        nullif(trim(`ペースアップ位置`), '') as `ペースアップ位置`,
        nullif(trim(`トラックバイアス_１角`), '') as `トラックバイアス_１角`,
        nullif(trim(`トラックバイアス_２角`), '') as `トラックバイアス_２角`,
        nullif(trim(`トラックバイアス_向正`), '') as `トラックバイアス_向正`,
        nullif(trim(`トラックバイアス_３角`), '') as `トラックバイアス_３角`,
        nullif(trim(`トラックバイアス_４角`), '') as `トラックバイアス_４角`,
        nullif(trim(`トラックバイアス_直線`), '') as `トラックバイアス_直線`,
        nullif(trim(`レースコメント`), '') as `レースコメント`,
        -- Audit columns
        file_name as _file_name,
        sha256 as _sha256
    from
        source
    )

select * from final
