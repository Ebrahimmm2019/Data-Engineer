CREATE OR REPLACE EXTERNAL TABLE `dt2023.dezoomcamp.fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://week3data/fhv_tripdata_2019-*']
);
CREATE TABLE `dt2023.dezoomcamp.fhv_tripdata_normal` AS
SELECT * FROM dt2023.dezoomcamp.fhv_tripdata;

SELECT count(*) FROM `dt2023.dezoomcamp.fhv_tripdata`;

SELECT COUNT(DISTINCT(Affiliated_base_number)) FROM `dt2023.dezoomcamp.fhv_tripdata`;

SELECT COUNT(DISTINCT(Affiliated_base_number)) FROM `dt2023.dezoomcamp.fhv_tripdata_normal`;

SELECT COUNT(*) FROM `dt2023.dezoomcamp.fhv_tripdata_normal`
 WHERE PUlocationID IS NULL and DOlocationID IS NULL;

 
CREATE OR REPLACE TABLE `dt2023.dezoomcamp.fhv_tripdata_partion`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS (
  SELECT * FROM `dt2023.dezoomcamp.fhv_tripdata_normal`
);

SELECT distinct (affiliated_base_number) FROM  `dt2023.dezoomcamp.fhv_tripdata_normal`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';

SELECT distinct (affiliated_base_number) FROM  `dt2023.dezoomcamp.fhv_tripdata_partion`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31'; 


