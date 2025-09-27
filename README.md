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

## Setting up hive metastore

Install hadoop

```
brew install hadoop
```

Download newest postgresql jdbc driver.

[Download hive](https://dlcdn.apache.org/hive/hive-3.1.3/).

Replace (hive-folder)/lib/postgresql-9.4.1208.jre7 with the newest postgresql jdbc driver.

Start postgres (init script will create metastore database)

```
docker compose up
```

Run schematool on metastore_db in postgres.

```
./apache-hive-3.1.3-bin/bin/schematool -dbType postgres -initSchema -url 'jdbc:postgresql://127.0.0.1:5432/metastore_db' -passWord admin -userName admin -driver org.postgresql.Driver -verbose
```

Start hive

```
make start_hive_server
```

## Data sources

### JRDB

![ER](./images/JRDB.drawio.png)

#### 翻訳方針

時間がかかり、ドキュメントと一致しなくなるため、原則カラム名などは日本語のままにする。ただし、技術的な制約により、カラム名などを英語に変更することがある。最終的なデータセットのカラム名は、日本語のみになる。

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
1. 同じ開催キーのレースは基本的に同じ日に行われるが、天気によって二日後など延期されることが稀にある（例: 2011年1小倉7）。そのため、開催キー＋年月日が本当のキーとなる。例:

| 開催キー | 場コード | 年  | 回  | 日  | 年月日     |
| -------- | -------- | --- | --- | --- | ---------- |
| 101117   | 10       | 11  | 1   | 7   | 2011-02-12 |
| 101117   | 10       | 11  | 1   | 7   | 2011-02-14 |

2. 直前データ／直前累積データの場合 競馬開催日 各レース出走１５分前頃。仕様書では、15分前となっていますが、10分前になってしまうこともあります。（コロナ渦以降の運用体制のため）(verified, sometimes it just comes in late, but its there beforehand. Must download from bottom of race day tab in http://www.jrdb.com/member/n_index.html). 直前累積データ最終版の場合 競馬開催日 全レース終了後、午後5:00頃.


#### Key descriptions

| キー         | 構成                                                                 |
| ------------ | -------------------------------------------------------------------- |
| レースキー   | 「場コード・年・回・日・Ｒ」の組み合わせ                             |
| 馬番         | 1-16など                                                             |
| 血統登録番号 | 99101712　など                                                       |
| 開催キー     | 「場コード・年・回・日」の組み合わせ。レースキーの一部とリンク可能。 |


#### Duplicates

Some datasets have identitical rows. Others have rows that are identical except for specific columns. The following table shows which datasets have duplicates and which columns are different.

| dataset | duplicates | different columns | notes                                                                                                      |
| ------- | ---------- | ----------------- | ---------------------------------------------------------------------------------------------------------- |
| BAC     | yes        | 年月日            | E.g., see 開催キー in '061345', '091115', '101125'. Keep rows with the latest date (verified in netkeiba). |
| CHA     |            |                   | todo                                                                                                       |
| CYB     |            |                   | todo                                                                                                       |
| HJC     |            |                   | todo                                                                                                       |
| KAB     |            |                   | todo                                                                                                       |
| KYI     |            |                   | todo                                                                                                       |
| OZ      |            |                   | todo                                                                                                       |
| OT      |            |                   | todo                                                                                                       |
| OU      |            |                   | todo                                                                                                       |
| OV      |            |                   | todo                                                                                                       |
| OW      |            |                   | todo                                                                                                       |
| SED     |            |                   | todo                                                                                                       |
| SKB     |            |                   | todo                                                                                                       |
| TYB     |            |                   | todo                                                                                                       |
| UKC     | yes        |                   | Identical rows, take whichever you please                                                                  |


#### Other data notes

bacとkyi/sedの頭数が1頭で異なる場合があるけど、あまりにも少ないので無視することにする。`runner_count_difference`に特定するためのクエリを書いておく。

| レースキー | 頭数_kyi | 頭数_bac |
| ---------- | -------- | -------- |
| 05041405   | 13       | 14       |
| 08004402   | 13       | 14       |
| 08004409   | 8        | 9        |
| 08085302   | 15       | 14       |

*騎手の乗り替わり*

SED、TYB、KYIに騎手コードが入っていますが、乗り替わりがある場合、一致しないことがある。SEDとTYBの差は66件、KYIとは4887件。乗り替わりの影響を最小限にするために、予測時にTYBの騎手コードを使うと良さそう。

See `sed_kyi_tyb_jockey_diff.sql` analysis.

*調教師の変更*

SEDとKYIで調教師が異なるケースが35件ある。

See `sed_kyi_trainer_diff.sql` analysis.

*sed/bac の距離の違い*

There are 2 races (from 2008-2009) where the distance is different between SED and BAC. So infrequent that we can ignore it.

See `sed_bac_distance_diff.sql` analysis.

*sed/kab の馬場状態の違い*

The horse track condition is usually the same or 1 off between SED and KAB.

*同着*

同じレースで着順が同じの場合がある。


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

### What to optimize for (according to ChatGPT)

In theory, we should optimize for precision, because we are looking for quality over quantity. In practice, precision seems to lead to higher payout rates. We may not bet on as many horses, but the quality of the bets should be higher.

In the context of predicting winning horses where your goal is to maximize the identification of true winners while minimizing bets on losing horses, the choice between loss, recall, precision, or the F1 score for your optimization function depends on your specific objectives and the trade-offs you are willing to make. Let's consider each option:

#### Precision
* **Minimizing False Positives is Critical:** In scenarios where the cost of a false positive is high, precision becomes more important. For example, in spam detection, a high precision rate means that fewer legitimate emails are incorrectly marked as spam, avoiding potential inconvenience or loss of important information.
* **Quality Over Quantity:** In applications like search engines or recommendation systems, where user experience can be significantly affected by the relevance of the results, optimizing for precision ensures that the results presented to the user are more likely to be relevant, enhancing user satisfaction.

#### Loss
- The loss function quantifies the model's prediction errors and is crucial during the training phase to guide the model towards better performance. However, loss functions like cross-entropy don't directly correspond to business objectives like maximizing recall or precision.
- Use loss for initial model training, but you might need additional metrics (like recall, precision, or F1 score) to fine-tune your model according to your specific objectives.

#### Recall
* **Minimizing False Negatives is Critical:** In applications where missing a positive instance is more costly than falsely labeling a negative instance as positive, recall is the priority. For example, in medical diagnostics for serious diseases, it's crucial to identify as many true cases (positive instances) as possible, even at the risk of some false positives. A high recall ensures that fewer actual cases of the disease go undetected.
* **Comprehensive Coverage is Required:** In information retrieval or legal discovery (e-discovery), where the goal is to find all relevant documents or information, optimizing for recall might be more important to ensure no critical information is overlooked.

### When to not bet

Don't bet on races where:
* The bac.レース条件_条件 is A1,A2 (these are new horse races)
* トラック種別 is 障害


### コーナー通過順位の読み方

```
各馬の前後間隔を以下の記号で表示します。
「（）」は1馬身未満の差で併走している馬群を示し、（）内は内側の馬から記します。
「*」は馬群の先頭馬を示します。
「,」は先行馬から1馬身以上2馬身未満の差を示します。
「-」は先行馬から2馬身以上5馬身未満の差を示します。
「=」は先行馬から5馬身以上の差を示します。
```

Example:

| Corner | Position |
| ------ | -------- |
| 1      | `(*2,10,15)(4,6,11,12)(1,7,8,14)(5,13)9-3` |
| 2     | `(2,*15)-(4,10)11,6(1,12)(7,14)9,8(5,13)3` |
| 3     | `15,2(10,11)(4,6,12,14)1(7,5,3)9,8,13` |
| 4     | `(*15,2)-(4,10)1(12,11)3(6,5,14)(9,7)-8,13` |
