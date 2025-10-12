from typing import List

from jhra.models.base import Feature


def get_features() -> List[Feature]:
    return [
        Feature(name="レースキー", pandas_dtype="string", tags=[]),
        Feature(name="馬番", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="枠番", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="血統登録番号", pandas_dtype="string", tags=[]),
        Feature(name="場コード", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="騎手コード", pandas_dtype="string", tags=[]),
        Feature(name="調教師コード", pandas_dtype="string", tags=[]),
        Feature(name="年月日", pandas_dtype="datetime64[ns]", tags=[]),
        Feature(name="頭数", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="単勝的中", pandas_dtype="category", tags=[]),
        Feature(name="単勝払戻金", pandas_dtype="float", tags=[]),
        Feature(name="複勝的中", pandas_dtype="category", tags=[]),
        Feature(name="複勝払戻金", pandas_dtype="float", tags=[]),
        Feature(name="四半期", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="瞬発戦好走馬_芝", pandas_dtype="category", tags=["default", "芝"]),
        Feature(name="消耗戦好走馬_芝", pandas_dtype="category", tags=["default", "芝"]),
        Feature(name="瞬発戦好走馬_ダート", pandas_dtype="category", tags=["ダート"]),
        Feature(name="消耗戦好走馬_ダート", pandas_dtype="category", tags=["ダート"]),
        Feature(name="瞬発戦好走馬_総合", pandas_dtype="category", tags=[]),
        Feature(name="消耗戦好走馬_総合", pandas_dtype="category", tags=[]),
        Feature(name="性別", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="馬場差", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="芝馬場状態内", pandas_dtype="category", tags=["default", "芝"]),
        Feature(name="芝馬場状態中", pandas_dtype="category", tags=["default", "芝"]),
        Feature(name="芝馬場状態外", pandas_dtype="category", tags=["default", "芝"]),
        Feature(name="直線馬場差最内", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="直線馬場差内", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="直線馬場差中", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="直線馬場差外", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="直線馬場差大外", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="ダ馬場状態内", pandas_dtype="category", tags=["ダート"]),
        Feature(name="ダ馬場状態中", pandas_dtype="category", tags=["ダート"]),
        Feature(name="ダ馬場状態外", pandas_dtype="category", tags=["ダート"]),
        Feature(name="芝種類", pandas_dtype="float", tags=["default", "芝"]),
        Feature(name="草丈", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="転圧", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="凍結防止剤", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="中間降水量", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="馬場状態コード", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(
            name="レース条件_トラック情報_右左",
            pandas_dtype="category",
            tags=["default", "芝", "ダート"],
        ),
        Feature(
            name="レース条件_トラック情報_内外",
            pandas_dtype="category",
            tags=["default", "芝", "ダート"],
        ),
        Feature(name="レース条件_種別", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="レース条件_条件", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="レース条件_記号", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="レース条件_重量", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(
            name="レース条件_グレード", pandas_dtype="category", tags=["default", "芝", "ダート"]
        ),
        Feature(name="トラック種別", pandas_dtype="category", tags=[]),
        Feature(name="ＩＤＭ", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="IDM標準偏差", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="IDM_標準偏差比", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="脚質", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="単勝オッズ", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="複勝オッズ", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="騎手指数", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="情報指数", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="オッズ指数", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="パドック指数", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="総合指数", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="馬具変更情報", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="脚元情報", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="負担重量", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="見習い区分", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="オッズ印", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="パドック印", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="直前総合印", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="馬体コード", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="気配コード", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="距離適性", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="上昇度", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="ローテーション", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="基準オッズ", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="基準人気順位", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="基準複勝オッズ", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="基準複勝人気順位", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="特定情報◎", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="特定情報○", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="特定情報▲", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="特定情報△", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="特定情報×", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="総合情報◎", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="総合情報○", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="総合情報▲", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="総合情報△", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="総合情報×", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="人気指数", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="調教指数", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="厩舎指数", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="調教矢印コード", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="厩舎評価コード", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="騎手期待連対率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="激走指数", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="蹄コード", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="重適性コード", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="クラスコード", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="ブリンカー", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="印コード_総合印", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(
            name="印コード_ＩＤＭ印", pandas_dtype="category", tags=["default", "芝", "ダート"]
        ),
        Feature(name="印コード_情報印", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="印コード_騎手印", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="印コード_厩舎印", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="印コード_調教印", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="印コード_激走印", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(
            name="展開予想データ_テン指数", pandas_dtype="float", tags=["default", "芝", "ダート"]
        ),
        Feature(
            name="展開予想データ_ペース指数", pandas_dtype="float", tags=["default", "芝", "ダート"]
        ),
        Feature(
            name="展開予想データ_上がり指数", pandas_dtype="float", tags=["default", "芝", "ダート"]
        ),
        Feature(
            name="展開予想データ_位置指数", pandas_dtype="float", tags=["default", "芝", "ダート"]
        ),
        Feature(
            name="展開予想データ_ペース予想", pandas_dtype="category", tags=["default", "芝", "ダート"]
        ),
        Feature(
            name="展開予想データ_道中順位", pandas_dtype="float", tags=["default", "芝", "ダート"]
        ),
        Feature(name="展開予想データ_道中差", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(
            name="展開予想データ_道中内外", pandas_dtype="category", tags=["default", "芝", "ダート"]
        ),
        Feature(
            name="展開予想データ_後３Ｆ順位", pandas_dtype="float", tags=["default", "芝", "ダート"]
        ),
        Feature(
            name="展開予想データ_後３Ｆ差", pandas_dtype="float", tags=["default", "芝", "ダート"]
        ),
        Feature(
            name="展開予想データ_後３Ｆ内外", pandas_dtype="category", tags=["default", "芝", "ダート"]
        ),
        Feature(
            name="展開予想データ_ゴール順位", pandas_dtype="float", tags=["default", "芝", "ダート"]
        ),
        Feature(
            name="展開予想データ_ゴール差", pandas_dtype="float", tags=["default", "芝", "ダート"]
        ),
        Feature(
            name="展開予想データ_ゴール内外", pandas_dtype="category", tags=["default", "芝", "ダート"]
        ),
        Feature(
            name="展開予想データ_展開記号", pandas_dtype="category", tags=["default", "芝", "ダート"]
        ),
        Feature(name="激走順位", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="LS指数順位", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="テン指数順位", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="ペース指数順位", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="上がり指数順位", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="位置指数順位", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="騎手期待単勝率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="騎手期待３着内率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="輸送区分", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="体型_全体", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="体型_背中", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="体型_胴", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="体型_尻", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="体型_トモ", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="体型_腹袋", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="体型_頭", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="体型_首", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="体型_胸", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="体型_肩", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="体型_前長", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="体型_後長", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="体型_前幅", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="体型_後幅", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="体型_前繋", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="体型_後繋", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="体型総合１", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="体型総合２", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="体型総合３", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="馬特記１", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="馬特記２", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="馬特記３", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(
            name="展開参考データ_馬スタート指数", pandas_dtype="float", tags=["default", "芝", "ダート"]
        ),
        Feature(
            name="展開参考データ_馬出遅率", pandas_dtype="float", tags=["default", "芝", "ダート"]
        ),
        Feature(name="万券指数", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="万券印", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="激走タイプ", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(
            name="休養理由分類コード", pandas_dtype="category", tags=["default", "芝", "ダート"]
        ),
        Feature(name="芝ダ障害フラグ", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="距離フラグ", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="クラスフラグ", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="転厩フラグ", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="去勢フラグ", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="乗替フラグ", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="放牧先ランク", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="厩舎ランク", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="天候コード", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="前走着順", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="前々走着順", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="前々々走着順", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="前走トップ3", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="前走枠番", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="入厩何日前", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="入厩15日未満", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="入厩35日以上", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="馬体重", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="馬体重増減", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="距離", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="前走距離差", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="年齢", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="4歳以下", pandas_dtype="category", tags=["default", "芝", "ダート"]),
        Feature(name="4歳以下頭数", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="4歳以下割合", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="レース数", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="1位完走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="トップ3完走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="1位完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="トップ3完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="場所レース数", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="場所1位完走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="場所トップ3完走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="場所1位完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="場所トップ3完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="トラック種別レース数", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="トラック種別1位完走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(
            name="トラック種別トップ3完走", pandas_dtype="float", tags=["default", "芝", "ダート"]
        ),
        Feature(name="トラック種別1位完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(
            name="トラック種別トップ3完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]
        ),
        Feature(name="馬場状態レース数", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="馬場状態1位完走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="馬場状態トップ3完走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="馬場状態1位完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="馬場状態トップ3完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="距離レース数", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="距離1位完走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="距離トップ3完走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="距離1位完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="距離トップ3完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="四半期レース数", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="四半期1位完走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="四半期トップ3完走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="四半期1位完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="四半期トップ3完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="騎手レース数", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="騎手1位完走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="騎手トップ3完走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="騎手1位完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="騎手トップ3完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="騎手場所レース数", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="騎手場所1位完走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="騎手場所トップ3完走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="騎手場所1位完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="騎手場所トップ3完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="騎手距離レース数", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="騎手距離1位完走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="騎手距離トップ3完走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="騎手距離1位完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="騎手距離トップ3完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="調教師レース数", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="調教師1位完走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="調教師トップ3完走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="調教師1位完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="調教師トップ3完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="調教師場所レース数", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="調教師場所1位完走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="調教師場所トップ3完走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="調教師場所1位完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(
            name="調教師場所トップ3完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]
        ),
        Feature(name="過去3走順位平方和", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="本賞金累計", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="1位完走平均賞金", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="レース数平均賞金", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="調教師本賞金累計", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="調教師1位完走平均賞金", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="調教師レース数平均賞金", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="騎手本賞金累計", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="騎手1位完走平均賞金", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="騎手レース数平均賞金", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="馬騎手レース数", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="馬騎手1位完走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="馬騎手1位完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="馬騎手トップ3完走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="馬騎手トップ3完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="馬騎手初二走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="馬騎手同騎手", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="馬騎手場所レース数", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="馬騎手場所1位完走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="馬騎手場所1位完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="馬騎手場所トップ3完走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(
            name="馬騎手場所トップ3完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]
        ),
        Feature(name="馬調教師レース数", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="馬調教師1位完走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="馬調教師1位完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="馬調教師トップ3完走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="馬調教師トップ3完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="馬調教師初二走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="馬調教師同調教師", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="馬調教師場所レース数", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="馬調教師場所1位完走", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="馬調教師場所1位完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(
            name="馬調教師場所トップ3完走", pandas_dtype="float", tags=["default", "芝", "ダート"]
        ),
        Feature(
            name="馬調教師場所トップ3完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]
        ),
        Feature(name="過去5走勝率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="過去5走トップ3完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(name="騎手過去5走勝率", pandas_dtype="float", tags=["default", "芝", "ダート"]),
        Feature(
            name="騎手過去5走トップ3完走率", pandas_dtype="float", tags=["default", "芝", "ダート"]
        ),
    ]


class Features:
    @staticmethod
    def get_pandas_dtypes():
        return {feature.name: feature.pandas_dtype for feature in get_features()}

    @staticmethod
    def get_feature_names_by_tag(tag: str) -> List[str]:
        return [feature.name for feature in get_features() if tag in feature.tags]

    @staticmethod
    def get_feature_names_by_any_tags_and_dtype(
        any_tags: List[str], dtype: str
    ) -> List[str]:
        """
        Returns feature names that have any tags.
        """
        feature_names = []
        for feature in get_features():
            if dtype == feature.pandas_dtype:
                for tag in any_tags:
                    if tag in feature.tags:
                        feature_names.append(feature.name)
                        break
        return feature_names

    @staticmethod
    def get_label() -> str:
        return "複勝的中"
