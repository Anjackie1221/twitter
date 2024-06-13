-- The first following code are used to create tables from S3 bucket
CREATE EXTERNAL TABLE IF NOT EXISTS `cancer`.`cancer_table` ( `tweet` string, `public_metrics` int, `label` int, `time` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = '\t')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://b18-andrew/big_data/cleaned_data/'
TBLPROPERTIES ('classification' = 'csv')

CREATE EXTERNAL TABLE IF NOT EXISTS `cancer`.`lr_pre` ( `tweet` string, `label` integer, `public_metrics` integer, `hours` string, `hours_index` double, `prediction` double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = '\t')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://b18-andrew/big_data/predict_data/athena/lr_pre/'
TBLPROPERTIES ('classification' = 'csv')

CREATE EXTERNAL TABLE IF NOT EXISTS `cancer`.`dt_pre` ( `tweet` string, `label` integer, `public_metrics` integer, `hours` string, `hours_index` double, `prediction` double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = '\t')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://b18-andrew/big_data/predict_data/athena/dt_pre/'
TBLPROPERTIES ('classification' = 'csv')

CREATE EXTERNAL TABLE IF NOT EXISTS `cancer`.`rf_pre` ( `tweet` string, `label` integer, `public_metrics` integer, `hours` string, `hours_index` double, `prediction` double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = '\t')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://b18-andrew/big_data/predict_data/athena/rf_pre/'
TBLPROPERTIES ('classification' = 'csv')

CREATE EXTERNAL TABLE IF NOT EXISTS `cancer`.`rawCancer` ( `tweetId` bigint, `name` string, `username` string, `tweet` string, `public_metrics` string, `location` string, `geo` string, `created_at` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = '\t')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://b18-andrew/big_data/raw_data/'
TBLPROPERTIES ('classification' = 'csv')

-- ***********************************************************

-- create a table to show the total tweet count for each label, and add name to each lable
create table sentiment_count as
select t."sentiment",count(t."sentiment") as total
from
(select "label",
    case when "label" = 2 then 'positive'
    when "label" = 1 then 'negative'
    else 'neutral'
    end as "sentiment"
from "cancer_table") as t
group by t."sentiment"

-- create a table to show top 15 follower based on their total tweets
create table following_rank as
select "public_metrics",count("tweet") as tweet_counts
from "cancer_table"
where "public_metrics" > 0
group by "public_metrics"
order by 2 desc
limit 15

-- extract hours from time columns, and group tweets on each hour
create table hour_rank as
select t.formatted_time, count(t.tweet) as "tweet_counts"
from
(select tweet,
    hour(date_parse("time",'%a %b %e %T') + interval '1' day) as formatted_time
from "cancer_table"
where time <> 'None') as t
group by 1
order by 1

-- calculate the accuracy for each algorithm and combine the 3 results into one table
create table accuracy as
select
(select round(cast(true_ as double) / total,4)
from
(select count(*) as total,
sum(case when label = "prediction" then 1 end) as true_
from "lr_pre") as t) as lr_accuracy,
(select round(cast(true_ as double) / total,4)
from
(select count(*) as total,
sum(case when label = "prediction" then 1 end) as true_
from "dt_pre") as t1) as dt_accuracy,
(select round(cast(true_ as double) / total,4)
from
(select count(*) as total,
sum(case when label = "prediction" then 1 end) as true_
from "rf_pre") as t1) as rf_accuracy

-- extract general location from location column
-- then select top 10 locations based on the total number of tweets
create table loc_rank as
select loc, count(tweet) as counts
from
(select tweet,location,
    trim(substr(location, strpos(location,',') + 1, length(location))) as loc
from "rawcancer") as tem
where loc <> 'None' and loc <> 'USA' and loc <> 'United States'
group by 1
order by 2 desc
limit 10;

-- split each tweet into different word and group by each word to make a word cloud visualization
create table word_count as
select names, count(*) as cnt
from "cancer_table"
cross join unnest(split(tweet,' ')) as t(names)
group by names;
