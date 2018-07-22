### Ranking articles in ukwiki by volatility
#### Goal
Create a ranking of relevant articles that exists in the Ukranian Wikipedia (ukwiki) but are missing in the English Wikipedia (enwiki) with ranking by volatility.

#### Data

The following data were used for the project:
* ukwiki-20180701-langlinks.sql.gz
* ukwiki-20180701-page.sql.gz
* pageviews-20180601-000000.gz
* ...
* pageviews-20180614-230000.gz

source: 
* https://dumps.wikimedia.org/ukwiki/20180701/
* https://dumps.wikimedia.org/other/pageviews/2018/2018-06/ (from 20180601 to 20180614)

To download files:
```bash
wget https://dumps.wikimedia.org/ukwiki/20180701/ukwiki-20180701-langlinks.sql.gz
wget https://dumps.wikimedia.org/ukwiki/20180701/ukwiki-20180701-page.sql.gz
wget https://dumps.wikimedia.org/other/pageviews/2018/2018-06/pageviews-201806{01..30}-{00..23}0000.gz
```
#### Preprocessing
Python script to convert sql.gz file to csv:
* https://github.com/arxitekton/mmds/blob/master/preprocessing/langlinks_sql2csv.py
* https://github.com/arxitekton/mmds/blob/master/preprocessing/page_sql2csv.py

#### ETL:
* https://github.com/arxitekton/mmds/tree/master/spark-wiki

##### getting pageviews stats:
* https://github.com/arxitekton/mmds/blob/master/spark-wiki/src/main/scala/com/mmds/demo/pageviewRequest_0.scala

algorithm:
* getting 'langlinks' data and filter $"lang" ==="en"
* getting 'page' data
* from 20180601 to 20180614:
    * getting hourly "pageviews" for this date and filter $"project" === "uk"
    * left join "pageviews" with 'page' on 'title' because we need $"page_id"
    * leftanti with 'langlinks' (exclude corresponding row) - because we need just article missing in the enwiki
    * groupBy "page_id" and aggregate sum of "requests"
    * append to parquet

##### getting variance:
* https://github.com/arxitekton/mmds/blob/master/spark-wiki/src/main/scala/com/mmds/demo/variance_ts.scala

#### Result
* https://github.com/arxitekton/mmds/tree/master/ranking.csv

page_id | title | variance
------- | ----- | --------
2331555|Леа_Фенер|0.0
1795506|Ємець_Іван_Артемович|0.0
1795507|Ємець_Іван_Артемович|0.0
1429206|Нова_Вирбовка|0.0
216681|Жаров|0.07142857142857141
2066733|Козаченко_Ігор_Іванович|0.07142857142857142
1832293|Всесвітній_фонд_природи|0.07142857142857142
2229121|Павол_Янік|0.07142857142857142
472587|Елмор_Джеймс|0.18131868131868126
472353|Елмор_Джеймс|0.1813186813186813

