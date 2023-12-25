with source as (
      select * from {{ source('jrdb', 'kyi') }}
),
final as (
    select
        concat(
            nullif("レースキー_場コード", ''),
            nullif("レースキー_年", ''),
            nullif("レースキー_回", ''),
            nullif("レースキー_日", ''),
            nullif("レースキー_Ｒ", '')
        ) as "レースキー",
        nullif("レースキー_場コード", '') as "レースキー_場コード",
        nullif("レースキー_年", '') as "レースキー_年",
        nullif("レースキー_回", '') as "レースキー_回",
        nullif("レースキー_日", '') as "レースキー_日",
        nullif("レースキー_Ｒ", '') as "レースキー_Ｒ",
        nullif("馬番", '') as "馬番",
        nullif("血統登録番号", '') as "血統登録番号",
        nullif("馬名", '') as "馬名",
        cast(nullif("ＩＤＭ", '') as numeric) as "ＩＤＭ",
        cast(nullif("騎手指数", '') as numeric) as "騎手指数",
        cast(nullif("情報指数", '') as numeric) as "情報指数",
        cast(nullif("総合指数", '') as numeric) as "総合指数",
        nullif("脚質", '') as "脚質",
        
        -- contains values 0-9, but 距離適性コード only lists 1,2,3,5,6
        case when "距離適性" in (SELECT code FROM {{ ref('距離適性コード') }}) then "距離適性" else null end "距離適性",

        -- 1-5 is expected, but there are many 0s and rarely 8s. Replacing with null.
        case when "上昇度" in (SELECT code FROM {{ ref('上昇度コード') }}) then "上昇度" else null end "上昇度",

        cast(nullif("ローテーション", '') as integer) as "ローテーション",
        cast(nullif("基準オッズ", '') as numeric) as "基準オッズ",
        cast(nullif("基準人気順位", '') as integer) as "基準人気順位",
        cast(nullif("基準複勝オッズ", '') as numeric) as "基準複勝オッズ",
        cast(nullif("基準複勝人気順位", '') as integer) as "基準複勝人気順位",
        cast(nullif("特定情報◎", '') as integer) as "特定情報◎",
        cast(nullif("特定情報○", '') as integer) as "特定情報○",
        cast(nullif("特定情報▲", '') as integer) as "特定情報▲",
        cast(nullif("特定情報△", '') as integer) as "特定情報△",
        cast(nullif("特定情報×", '') as integer) as "特定情報×",
        cast(nullif("総合情報◎", '') as integer) as "総合情報◎",
        cast(nullif("総合情報○", '') as integer) as "総合情報○",
        cast(nullif("総合情報▲", '') as integer) as "総合情報▲",
        cast(nullif("総合情報△", '') as integer) as "総合情報△",
        cast(nullif("総合情報×", '') as integer) as "総合情報×",
        cast(nullif("人気指数", '') as integer) as "人気指数",
        cast(nullif("調教指数", '') as numeric) as "調教指数",
        cast(nullif("厩舎指数", '') as numeric) as "厩舎指数",

        -- replacing 2 nulls with most common value
        coalesce(nullif("調教矢印コード", ''), '3') as "調教矢印コード",

        -- replacing 2 nulls with most common value
        coalesce(nullif("厩舎評価コード", ''), '3') as "厩舎評価コード",

        cast(nullif("騎手期待連対率", '') as numeric) as "騎手期待連対率",
        cast(nullif("激走指数", '') as integer) as "激走指数",

        -- A handful of records contain codes 00, 13, 16, 25, and 45, which are missing from the codes list
        -- defaulting to null for these edge cases
        case when "蹄コード" in ('00', '13', '16', '25', '45', '') then null else "蹄コード" end "蹄コード",

        -- 0 is not included in schema, but null is.
        nullif(nullif("重適性コード", ''), '0') as "重適性コード",

        -- Same story here. Codes appear in the data that aren't in the schema. Replacing with null.
        case when "クラスコード" in ('00', '19', '20', '63', '64', '65', '66', '') then null else "クラスコード" end "クラスコード",

        nullif("ブリンカー", '') as "ブリンカー",
        nullif("騎手名", '') as "騎手名",
        cast(nullif("負担重量", '') as integer) as "負担重量",
        nullif("見習い区分", '') as "見習い区分",
        nullif("調教師名", '') as "調教師名",
        nullif("調教師所属", '') as "調教師所属",
        nullif("他データリンク用キー_前走１競走成績キー", '') as "他データリンク用キー_前走１競走成績キー",
        nullif("他データリンク用キー_前走２競走成績キー", '') as "他データリンク用キー_前走２競走成績キー",
        nullif("他データリンク用キー_前走３競走成績キー", '') as "他データリンク用キー_前走３競走成績キー",
        nullif("他データリンク用キー_前走４競走成績キー", '') as "他データリンク用キー_前走４競走成績キー",
        nullif("他データリンク用キー_前走５競走成績キー", '') as "他データリンク用キー_前走５競走成績キー",
        nullif("他データリンク用キー_前走１レースキー", '') as "他データリンク用キー_前走１レースキー",
        nullif("他データリンク用キー_前走２レースキー", '') as "他データリンク用キー_前走２レースキー",
        nullif("他データリンク用キー_前走３レースキー", '') as "他データリンク用キー_前走３レースキー",
        nullif("他データリンク用キー_前走４レースキー", '') as "他データリンク用キー_前走４レースキー",
        nullif("他データリンク用キー_前走５レースキー", '') as "他データリンク用キー_前走５レースキー",
        nullif("枠番", '') as "枠番",

        -- The code 7 is included in the data pretty frequently, but not in the schema. Replacing with null.
        nullif(nullif("印コード_総合印", ''), '7') as "印コード_総合印",
        nullif(nullif("印コード_ＩＤＭ印", ''), '7') as "印コード_ＩＤＭ印",
        nullif(nullif("印コード_情報印", ''), '7') as "印コード_情報印",
        nullif(nullif("印コード_騎手印", ''), '7') as "印コード_騎手印",
        nullif(nullif("印コード_厩舎印", ''), '7') as "印コード_厩舎印",
        nullif(nullif("印コード_調教印", ''), '7') as "印コード_調教印",
        coalesce(cast(nullif("印コード_激走印", '') as boolean), false) as "印コード_激走印",

        -- 4, 5, 6 appear in these columns very rarely, not in schema
        case when "芝適性コード" in ('1', '2', '3') then "芝適性コード" else null end "芝適性コード",
        case when "ダ適性コード" in ('1', '2', '3') then "ダ適性コード" else null end "ダ適性コード",

        -- todo: link with master
        nullif("騎手コード", '') as "騎手コード",
        nullif("調教師コード", '') as "調教師コード",

        -- has 14 nulls, replacing with 0
        coalesce(cast(nullif("賞金情報_獲得賞金", '') as integer), 0) as "賞金情報_獲得賞金",

        cast(nullif("賞金情報_収得賞金", '') as integer) as "賞金情報_収得賞金",

        -- has 14 nulls, replacing with 0
        coalesce(nullif("賞金情報_条件クラス", ''), '0') as "賞金情報_条件クラス",

        cast(nullif("展開予想データ_テン指数", '') as numeric) as "展開予想データ_テン指数",
        cast(nullif("展開予想データ_ペース指数", '') as numeric) as "展開予想データ_ペース指数",
        cast(nullif("展開予想データ_上がり指数", '') as numeric) as "展開予想データ_上がり指数",
        cast(nullif("展開予想データ_位置指数", '') as numeric) as "展開予想データ_位置指数",

        -- years 2019,2020 have values 0,1,2,3,4 but schema says only S,M,H
        -- the number of integers is only about 2000, so just setting to null instead
        case when "展開予想データ_ペース予想" in ('S', 'M', 'H') then "展開予想データ_ペース予想" else null end "展開予想データ_ペース予想",

        cast(nullif("展開予想データ_道中順位", '') as integer) as "展開予想データ_道中順位",
        cast(nullif("展開予想データ_道中差", '') as integer) as "展開予想データ_道中差",
        nullif("展開予想データ_道中内外", '') as "展開予想データ_道中内外",
        cast(nullif("展開予想データ_後３Ｆ順位", '') as integer) as "展開予想データ_後３Ｆ順位",
        cast(nullif("展開予想データ_後３Ｆ差", '') as integer) as "展開予想データ_後３Ｆ差",
        nullif("展開予想データ_後３Ｆ内外", '') as "展開予想データ_後３Ｆ内外",
        cast(nullif("展開予想データ_ゴール順位", '') as integer) as "展開予想データ_ゴール順位",
        cast(nullif("展開予想データ_ゴール差", '') as integer) as "展開予想データ_ゴール差",
        nullif("展開予想データ_ゴール内外", '') as "展開予想データ_ゴール内外",
        nullif("展開予想データ_展開記号", '') as "展開予想データ_展開記号",

        -- what does this field even mean?
        -- contains values (1,2,3,4,5,6,8), but 距離適性コード only lists 1,2,3,5,6
        case when "距離適性２" in (SELECT code FROM {{ ref('距離適性コード') }}) then "距離適性２" else null end "距離適性２",

        cast(nullif("枠確定馬体重", '') as integer) as "枠確定馬体重",
        nullif("枠確定馬体重増減", '') as "枠確定馬体重増減",
        cast(coalesce(nullif("取消フラグ", ''), '0') as boolean) as "取消フラグ",
        nullif("性別コード", '') as "性別コード",
        nullif("馬主名", '') as "馬主名",
        nullif("馬主会コード", '') as "馬主会コード",

        -- There are 17 rows with nulls in them.
        -- null is represented in the codes as 00, so replacing with that.
        coalesce(nullif("馬記号コード", ''), '00') as "馬記号コード",

        cast(nullif("激走順位", '') as integer) as "激走順位",
        cast(nullif("LS指数順位", '') as integer) as "LS指数順位",
        cast(nullif("テン指数順位", '') as integer) as "テン指数順位",
        cast(nullif("ペース指数順位", '') as integer) as "ペース指数順位",
        cast(nullif("上がり指数順位", '') as integer) as "上がり指数順位",
        cast(nullif("位置指数順位", '') as integer) as "位置指数順位",
        cast(nullif("騎手期待単勝率", '') as numeric) as "騎手期待単勝率",
        cast(nullif("騎手期待３着内率", '') as numeric) as "騎手期待３着内率",
        coalesce(nullif("輸送区分", ''), '0') as "輸送区分",
        nullif("走法", '') as "走法",


        case when substring("体型", 1, 1) in ('1', '2', '3') then substring("体型", 1, 1) else null end "体型_全体",
        case when substring("体型", 2, 1) in ('1', '2', '3') then substring("体型", 2, 1) else null end "体型_背中",
        case when substring("体型", 3, 1) in ('1', '2', '3') then substring("体型", 3, 1) else null end "体型_胴",
        case when substring("体型", 4, 1) in ('1', '2', '3') then substring("体型", 4, 1) else null end "体型_尻",
        case when substring("体型", 5, 1) in ('1', '2', '3') then substring("体型", 5, 1) else null end "体型_トモ",
        case when substring("体型", 6, 1) in ('1', '2', '3') then substring("体型", 6, 1) else null end "体型_腹袋",
        case when substring("体型", 7, 1) in ('1', '2', '3') then substring("体型", 7, 1) else null end "体型_頭",
        case when substring("体型", 8, 1) in ('1', '2', '3') then substring("体型", 8, 1) else null end "体型_首",
        case when substring("体型", 9, 1) in ('1', '2', '3') then substring("体型", 9, 1) else null end "体型_胸",
        case when substring("体型", 10, 1) in ('1', '2', '3') then substring("体型", 10, 1) else null end "体型_肩",
        case when substring("体型", 11, 1) in ('1', '2', '3') then substring("体型", 11, 1) else null end "体型_前長",
        case when substring("体型", 12, 1) in ('1', '2', '3') then substring("体型", 12, 1) else null end "体型_後長",
        case when substring("体型", 13, 1) in ('1', '2', '3') then substring("体型", 13, 1) else null end "体型_前幅",
        case when substring("体型", 14, 1) in ('1', '2', '3') then substring("体型", 14, 1) else null end "体型_後幅",
        case when substring("体型", 15, 1) in ('1', '2', '3') then substring("体型", 15, 1) else null end "体型_前繋",
        case when substring("体型", 16, 1) in ('1', '2', '3') then substring("体型", 16, 1) else null end "体型_後繋",

        -- don't use 体型_尾 and 体型_振 fields because they are almost always null
        substring("体型", 17, 1) as "体型_尾",
        substring("体型", 18, 1) as "体型_振",

        substring("体型", 19, 1) as "体型_予備1",
        substring("体型", 20, 1) as "体型_予備2",
        substring("体型", 21, 1) as "体型_予備3",
        substring("体型", 22, 1) as "体型_予備4",
        substring("体型", 23, 1) as "体型_予備5",
        substring("体型", 24, 1) as "体型_予備6",
        substring("体型", 25, 1) as "体型_予備7",
        substring("体型", 26, 1) as "体型_予備8",

        -- このフィールドに z とか h が入っていたりする
        case when "体型総合１" in (SELECT コード FROM {{ ref('特記コード') }}) then "体型総合１" else null end "体型総合１",
        case when "体型総合２" in (SELECT コード FROM {{ ref('特記コード') }}) then "体型総合２" else null end "体型総合２",
        case when "体型総合３" in (SELECT コード FROM {{ ref('特記コード') }}) then "体型総合３" else null end "体型総合３",
        case when "馬特記１" in (SELECT コード FROM {{ ref('特記コード') }}) then "馬特記１" else null end "馬特記１",
        case when "馬特記２" in (SELECT コード FROM {{ ref('特記コード') }}) then "馬特記２" else null end "馬特記２",
        case when "馬特記３" in (SELECT コード FROM {{ ref('特記コード') }}) then "馬特記３" else null end "馬特記３",

        cast(nullif("展開参考データ_馬スタート指数", '') as numeric) as "展開参考データ_馬スタート指数",
        cast(nullif("展開参考データ_馬出遅率", '') as numeric) as "展開参考データ_馬出遅率",
        nullif("展開参考データ_参考前走", '') as "展開参考データ_参考前走",
        nullif("展開参考データ_参考前走騎手コード", '') as "展開参考データ_参考前走騎手コード",
        cast(nullif("万券指数", '') as integer) as "万券指数",
        nullif("万券印", '') as "万券印",
        nullif("降級フラグ", '') as "降級フラグ",
        nullif("激走タイプ", '') as "激走タイプ",
        nullif("休養理由分類コード", '') as "休養理由分類コード",

        case when substring("フラグ", 1, 1) in ('0', '1', '2') then substring("フラグ", 1, 1) else null end "芝ダ障害フラグ",
        case when substring("フラグ", 2, 1) in ('0', '1') then substring("フラグ", 2, 1) else null end "距離フラグ",
        case when substring("フラグ", 3, 1) in ('0', '1', '2', '3') then substring("フラグ", 3, 1) else null end "クラスフラグ",
        case when substring("フラグ", 4, 1) in ('0', '1', '2', '3') then substring("フラグ", 4, 1) else null end "転厩フラグ",
        case when substring("フラグ", 5, 1) in ('0', '1', '2', '3') then substring("フラグ", 5, 1) else null end "去勢フラグ",
        case when substring("フラグ", 6, 1) in ('0', '1', '9') then substring("フラグ", 6, 1) else null end "乗替フラグ",
        substring("フラグ", 7, 10) "予備フラグ",
        cast(nullif("入厩何走目", '') as integer) as "入厩何走目",
        to_date(nullif("入厩年月日", ''), 'YYYYMMDD') as "入厩年月日",
        cast(nullif("入厩何日前", '') as integer) as "入厩何日前",
        nullif("放牧先", '') as "放牧先",
        nullif("放牧先ランク", '') as "放牧先ランク",
        nullif("厩舎ランク", '') as "厩舎ランク"
    from source
)
select * from final
  