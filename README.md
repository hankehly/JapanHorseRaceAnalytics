# JapanHorseRaceAnalytics

- [JRDBデータのご案内](http://www.jrdb.com/program/data.html)
- [ＪＲＤＢデータの種類と概要](http://www.jrdb.com/program/jrdb_data_doc.txt)
- [JRDBデータコード表](http://www.jrdb.com/program/jrdb_code.txt)
- [Data Index (restricted)](http://www.jrdb.com/member/dataindex.html)
- [馬券の種類：はじめての方へ](https://www.jra.go.jp/kouza/beginner/baken/)
- [PostgreSQL JDBC driver](https://jdbc.postgresql.org/download/)
- [dbt - How we structure our dbt projects](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview)
- [競走馬データ仕様書内容の説明](http://www.jrdb.com/program/Kyi/ky_siyo_doc.txt)
- [JRA レーシングカレンダー](https://www.jra.go.jp/keiba/calendar/)
- [JMAダウンロードファイル(CSVファイル)の形式](https://www.data.jma.go.jp/risk/obsdl/top/help3.html)
- [JMA、今の天候情報例](https://tenki.jp/forecast/3/15/4510/12204/1hour.html)
- [JRDB競馬読本Web](http://www.jrdb.com/dokuhon/menu.php)


## Data sources

### JRDB

![ER](./images/JRDB.drawio.png)

#### Data update schedule

![schedule](./images/schedule.png)


#### Table grain

| file |                                                                                                           | grain          | ユニークキー                             | 更新時間   | 実績/予測 |
| ---- | --------------------------------------------------------------------------------------------------------- | -------------- | ---------------------------------------- | ---------- | --------- |
| SED  | 成績分析用                                                                                                | 1 race + horse | レースキー・馬番・競走成績キー           | 木 17:00   | 成績情報  |
| SKB  | 成績分析用・拡張データ                                                                                    | 1 race + horse | レースキー・馬番・競走成績キー           | 木 17:00   | 成績情報  |
| BAC  | レース番組情報                                                                                            | 1 race         | レースキー                               | 金土	19:00 | 前日情報  |
| CYB  | 調教分析データ                                                                                            | 1 race + horse | レースキー・馬番                         | 金土	19:00 | 前日情報  |
| CHA  | 調教本追切データ                                                                                          | 1 race + horse | レースキー・馬番                         | 金土	19:00 | 前日情報  |
| KAB  | 馬場・天候予想等の開催に対するデータ                                                                      | 1 place/day    | 開催キー + 年月日 (*1)                   | 金土	19:00 | 前日情報  |
| KYI  | 競走馬ごとのデータ。IDM、各指数を格納、放牧先を追加                                                       | 1 race + horse | レースキー・馬番・血統登録番号           | 金土	19:00 | 前日情報  |
| OZ   | 単複・馬連の基準オッズデータ                                                                              | 1 race         | レースキー                               | 金土	19:00 | 前日情報  |
| OW   | ワイドの基準オッズデータ                                                                                  | 1 race         | レースキー                               | 金土	19:00 | 前日情報  |
| OU   | 馬単の基準オッズデータ                                                                                    | 1 race         | レースキー                               | 金土	19:00 | 前日情報  |
| OT   | ３連複の基準オッズデータ                                                                                  | 1 race         | レースキー                               | 金土	19:00 | 前日情報  |
| OV   | ３連単の基準オッズデータ                                                                                  | 1 race         | レースキー                               | 金土	19:00 | 前日情報  |
| UKC  | 馬に関するデータを格納                                                                                    | 1 horse        | 血統登録番号 + データ年月日 (SCD Type 2) | 金土	19:00 | 前日情報  |
| TYB  | 直前情報データ                                                                                            | 1 race + horse | レースキー・馬番                         | (*2)       | 当日情報  |
| HJC  | 払戻(実績)情報に関するデータを格納 (payout information, i.e. which horse won how much for what 馬券 type) | 1 race         | レースキー                               | 土日 17:00 | 当日情報  |

Notes:
1. 同じ開催キーのレースは基本的に同じ日に行われるが、天気によって二日後など延期されることが稀にある（例: 2011年1小倉7）。そのため、開催キー＋年月日が本当のキーとなる。
2. 1) 直前データ／直前累積データの場合 競馬開催日 各レース出走１５分前頃。仕様書では、15分前となっていますが、10分前になってしまうこともあります。（コロナ渦以降の運用体制のため）(verified, sometimes it just comes in late, but its there beforehand. Must download from bottom of race day tab in http://www.jrdb.com/member/n_index.html) 2) 直前累積データ最終版の場合 競馬開催日 全レース終了後、午後５：００頃


#### Key descriptions

| キー         | 構成                                                                 |
| ------------ | -------------------------------------------------------------------- |
| レースキー   | 「場コード・年・回・日・Ｒ」の組み合わせ                             |
| 馬番         | 1-16など                                                             |
| 血統登録番号 | 99101712　など                                                       |
| 開催キー     | 「場コード・年・回・日」の組み合わせ。レースキーの一部とリンク可能。 |


#### Duplicates

| Dataset | Verified range | Has duplicates? | Notes                                                  |
| ------- | -------------- | --------------- | ------------------------------------------------------ |
| bac     | ~2023/12/17    | [x]             |                                                        |
| cha     | ~2023/12/17    | [ ]             |                                                        |
| cyb     | ~2023/12/17    | [x]             |                                                        |
| hjc     | ~2023/12/17    | [ ]             |                                                        |
| kab     | ~2023/12/17    | [ ]             |                                                        |
| kyi     | ~2023/12/17    | [ ]             |                                                        |
| oz      | ~2023/12/17    | [ ]             |                                                        |
| sed     | ~2023/12/17    | [x]             |                                                        |
| skb     | ~2023/12/17    | [ ]             |                                                        |
| tyb     | ~2023/12/17    | [x]             |                                                        |
| ukc     | ~2023/12/17    | [x]             | May contain duplicates in new files prior to new races |


## Modeling methodology

### Metrics to track for each binary classifier model

* Payoff rate overall
* Payoff rate by surface
* Payoff rate by distance (short, medium, long)
* Payoff rate by weather
* Payoff rate by season
* Payoff rate by year
* Payoff rate by month
* Payoff rate by confidence level (if model offers confidence level)
* Payoff rate by horse age
* Payoff rate by grade
* accuracy
* precision
* recall
* f1 score
* roc auc
* confusion matrix
* feature importance


### When to not bet

Don't bet on races where:
* The bac.レース条件_条件 is A1,A2 (these are new horse races)
* トラック種別 is 障害
