---------------  Setup  --------------

create or replace warehouse LARGE with warehouse_size='LARGE';
create or replace database ml;
create or replace schema demo;

----------- Data Ingestion ------------

create or replace stage taxi_s3_stage url='s3://ikunen-sagemaker/' credentials=(aws_key_id='Your_AWS_Key_Id' aws_secret_key='Your_AWS_Key_Secret');
create or replace table chicago_taxi_raw (
  TripID STRING,
  TaxiID STRING,  	
  TripStart STRING,   	
  TripEnd STRING,   	
  TripSeconds INT,	
  TripMiles FLOAT,   	
  PickupCensusTract STRING,
  DropoffCensusTract STRING,   	
  PickupCommunityArea INT,  	
  DropoffCommunityArea INT,   	
  Fare FLOAT,   	
  Tips STRING,   	
  Tolls STRING,   	
  Extras STRING,  	
  TripTotal STRING,   	
  PaymentType STRING,   	
  Company STRING,  	
  PickupCentroidLatitude FLOAT,
  PickupCentroidLongitude FLOAT,
  PickupCentroidLocation STRING,
  DropoffCentroidLatitude FLOAT,   	
  DropoffCentroidLongitude FLOAT,   	
  DropoffCentroidLocation STRING,
  CommunityAreas INT
);

create or replace file format taxi type = CSV;
copy into chicago_taxi_raw from @taxi_s3_stage/chicago_taxi.csv file_format = (format_name = 'taxi' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY='"') ON_ERROR='CONTINUE';

select * from chicago_taxi_raw limit 5;
select count(*) from chicago_taxi_raw;


----------- Feature Selection -----------

create or replace table chicago_taxi as
select 
  TripID,
  TO_TIMESTAMP_NTZ(TRIPSTART, 'MM/DD/YYYY HH12:MI:SS AM') as TripStart,
  Fare,
  Company,  	
  PickupCommunityArea,
  DropoffCommunityArea,
  PickupCentroidLatitude,
  PickupCentroidLongitude,
  DropoffCentroidLatitude,   	
  DropoffCentroidLongitude
from chicago_taxi_raw
where true
  and PickupCentroidLatitude is not null
  and PickupCentroidLongitude is not null
  and DropoffCentroidLatitude is not null
  and DropoffCentroidLongitude is not null
  and fare is not null;
  

select * from chicago_taxi limit 5;


-----------  Preprocessing  ------------

-- step 1: convert timestamp into weekday and hour

Create or replace view chicago_taxi_preprocessed_v1 as
Select
   TripID,
   Fare,
   Company,
   PickupCentroidLatitude,
   PickupCentroidLongitude,
   DropoffCentroidLatitude,
   DropoffCentroidLongitude,
   DAYOFWEEK(TripStart) as TripStart_DayOfWeek,
   HOUR(TripStart) as TripStart_Hour,
   IFF(PickupCommunityArea is NULL, 0, PickupCommunityArea) as PickupCommunityArea,
   IFF(DropoffCommunityArea is NULL, 0, DropoffCommunityArea) as DropoffCommunityArea
from
   chicago_taxi;
   
-- step 2: normalize latitude and longtitude

create or replace view chicago_taxi_preprocessed_v2 as
select
   TripID,
   Fare,
   Company,
   TripStart_DayOfWeek,
   TripStart_Hour,
   PickupCommunityArea,
   DropoffCommunityArea,
   2 * (PickupCentroidLatitude - ((select max(PickupCentroidLatitude) from chicago_taxi) + (select min(PickupCentroidLatitude) from chicago_taxi)) / 2)
       / ((select max(PickupCentroidLatitude) from chicago_taxi) - (select min(PickupCentroidLatitude) from chicago_taxi)) as PickupCentroidLatitude,
   2 * (PickupCentroidLongitude - ((select max(PickupCentroidLongitude) from chicago_taxi) + (select min(PickupCentroidLongitude) from chicago_taxi)) / 2)
       / ((select max(PickupCentroidLongitude) from chicago_taxi) - (select min(PickupCentroidLongitude) from chicago_taxi)) as PickupCentroidLongitude, 
   2 * (DropoffCentroidLatitude - ((select max(DropoffCentroidLatitude) from chicago_taxi) + (select min(DropoffCentroidLatitude) from chicago_taxi)) / 2)
       / ((select max(DropoffCentroidLatitude) from chicago_taxi) - (select min(DropoffCentroidLatitude) from chicago_taxi)) as DropoffCentroidLatitude, 
   2 * (DropoffCentroidLongitude - ((select max(DropoffCentroidLongitude) from chicago_taxi) + (select min(DropoffCentroidLongitude) from chicago_taxi)) / 2)
       / ((select max(DropoffCentroidLongitude) from chicago_taxi) - (select min(DropoffCentroidLongitude) from chicago_taxi)) as DropoffCentroidLongitude
from
   chicago_taxi_preprocessed_v1;
   
   
-- step 3: convert taxi company (string) into index (int)
Create or replace table chicago_taxi_company_vocab as select Row_Number() OVER(ORDER BY Company ASC) as idx, Company from (select distinct(Company) from chicago_taxi);

select * from chicago_taxi_company_vocab;

create or replace table chicago_taxi_preprocessed as
select
   TripID,
   Fare,
   TripStart_DayOfWeek,
   TripStart_Hour,
   PickupCentroidLatitude,
   PickupCentroidLongitude, 
   DropoffCentroidLatitude, 
   DropoffCentroidLongitude,
   PickupCommunityArea,
   DropoffCommunityArea,
   chicago_taxi_company_vocab.idx as Company_Index
from
   chicago_taxi_preprocessed_v2
join
   chicago_taxi_company_vocab
on chicago_taxi_preprocessed_v2.company = chicago_taxi_company_vocab.company;

select * from chicago_taxi_preprocessed limit 5;

-- step 4: convert features into libsvm format. one-hot encoding on categorical columns.

set company_num = (select count(*) from chicago_taxi_company_vocab);
set community_num = (select count(distinct(PickupCommunityArea)) from chicago_taxi);
set community_num = $community_num + 1;

create or replace function to_libsvm(
  "Fare" double,
  "PickupCentroidLatitude" double,
  "PickupCentroidLongitude" double,
  "DropoffCentroidLatitude" double,
  "DropoffCentroidLongitude" double,
  "TripStart_DayOfWeek" double,
  "TripStart_Hour" double,
  "Company_Index" double,
  "PickupCommunityArea" double,
  "DropoffCommunityArea" double,
  "company_num" double,
  "community_num" double)
returns String
language javascript
as
$$

function feature_numeric(p_lat, p_long, d_lat, d_long)
{
  return ' 0:' + p_lat.toString() + ' 1:' + p_long.toString() + ' 2:' + d_lat.toString() + ' 3:' + d_long.toString();
}

function feature_tripstart_dayofweek(weekday)
{
  index = 4;
  return ' ' + (weekday + index).toString() + ':1';
}

function feature_tripstart_hour(hour)
{
  index = 4 + 7;
  return ' ' + (hour + index).toString() + ':1';
}

function feature_company(company)
{
  index = 4 + 7 + 24;
  return ' ' + (company + index).toString() + ':1';
}

function feature_pickup_area(pickup_area)
{
  index = 4 + 7 + 24 + company_num;
  return ' ' + (pickup_area + index).toString() + ':1';
}

function feature_dropoff_area(dropoff_area)
{
  index = 4 + 7 + 24 + company_num + community_num;
  return ' ' + (dropoff_area + index).toString() + ':1';
}

return Fare.toString() + ' ' +
         feature_numeric(PickupCentroidLatitude, PickupCentroidLongitude, DropoffCentroidLatitude, DropoffCentroidLongitude) + 
         feature_tripstart_dayofweek(TripStart_DayOfWeek) + feature_tripstart_hour(TripStart_Hour) + 
         feature_company(Company_Index) + feature_pickup_area(PickupCommunityArea) + feature_dropoff_area(DropoffCommunityArea);
$$;

create or replace table chicago_taxi_libsvm (libsvm STRING, TripID STRING, fare FLOAT) as select to_libsvm(
  FARE,
  PickupCentroidLatitude,
  PickupCentroidLongitude,
  DropoffCentroidLatitude,
  DropoffCentroidLongitude,
  TripStart_DayOfWeek,
  TripStart_Hour,
  Company_Index,
  PickupCommunityArea,
  DropoffCommunityArea,
  $company_num,
  $community_num), TripID, fare from chicago_taxi_preprocessed;
  
select * from chicago_taxi_libsvm limit 5;


-------------  Training  --------------

--- See notebook

select * from chicago_taxi_model;


-------------  Evaluation  --------------
create or replace function predict(
  "libsvm" string, "model" variant)
returns double
language javascript
as
$$

function getLeafFromTree(tree, features) {
  node = tree;
  leaf = 0.0;
  while(!("leaf" in node)) {
    featureId = parseInt(node['split'].substring(1));
    threshold = node['split_condition'];
    featureValue = 0.0;
    nodeId = 0;
    if (featureId in features)
    {
      featureValue = features[featureId];
      if (featureValue < threshold)
      {
        nodeId = node["yes"];
      }
      else
      {
      	nodeId = node["no"];
      }
    }
    else
    {
      nodeId = node["missing"];
    }
    nextNode = null;
    for (i = 0; i < node['children'].length; i++)
    {
      if (node['children'][i]['nodeid'] == nodeId)
      {
        nextNode = node['children'][i];
        break;
      }
    }
    if (nextNode == null)
    {
      return 0.0;
    }
    node = nextNode;
  }
  return node["leaf"]; 
}

function predict(libsvm, model)
{
  var features = {}
  parts = libsvm.split(" ");
  for (i = 0; i < parts.length; i++)
  {
    pair = parts[i].split(":");
    if (pair.length == 2)
    {
      k = parseInt(pair[0]);
      v = parseFloat(pair[1]);
      features[k] = v;
    }
  }
  total = 0.0;
  for(j = 0; j < model.length; j++)
  {
    total += getLeafFromTree(model[j], features);
  }
  return total;
}

return predict(libsvm, model);
$$;


-- test run
select predict(s.libsvm, m.model), s.fare, s.TripID from 
  (select libsvm, fare, TripID from chicago_taxi_libsvm limit 5) s
join chicago_taxi_model m 
where m.name='chicago_4_40.json';

-- select test sets and save them to table
create or replace table prediction_source (libsvm STRING, fare FLOAT, TripID STRING) as 
  (select libsvm, fare, TripID from chicago_taxi_libsvm limit 1000);

-- prediction with model1: depth 3, trees 20
create or replace table chicago_taxi_prediction_3_20 (prediction FLOAT, fare FLOAT, TripID STRING) as 
  (select predict(s.libsvm, m.model), s.fare, s.TripID from prediction_source s
  join chicago_taxi_model m
  where m.name='chicago_3_20.json');
  
select * from chicago_taxi_prediction_3_20 limit 5;
  
-- prediction with model2: depth 4, trees 20
create or replace table chicago_taxi_prediction_4_20 (prediction FLOAT, fare FLOAT, TripID STRING) as 
  (select predict(s.libsvm, m.model), s.fare, s.TripID from prediction_source s
  join chicago_taxi_model m
  where m.name='chicago_4_20.json');

-- prediction with model3: depth 4, trees 40
create or replace table chicago_taxi_prediction_4_40 (prediction FLOAT, fare FLOAT, TripID STRING) as 
  (select predict(s.libsvm, m.model), s.fare, s.TripID from prediction_source s
  join chicago_taxi_model m
  where m.name='chicago_4_40.json');

select stddev(fare) from prediction_source;
  
-- model1:
select SQRT(
(select sum((fare - prediction) * (fare - prediction)) from chicago_taxi_prediction_3_20) /
(select count(*) from chicago_taxi_prediction_3_20)) as RMSE;
  
-- model2:
select SQRT(
(select sum((fare - prediction) * (fare - prediction)) from chicago_taxi_prediction_4_20) /
(select count(*) from chicago_taxi_prediction_4_20)) as RMSE;

-- model3:
select SQRT(
(select sum((fare - prediction) * (fare - prediction)) from chicago_taxi_prediction_4_40) /
(select count(*) from chicago_taxi_prediction_4_40)) as RMSE;

-- analyzing model: group by weekday
select 
  SQRT(sum((chicago_taxi_prediction_4_40.fare - prediction) * (chicago_taxi_prediction_4_40.fare - prediction)) / count(*)) as RMSE,
  DAYOFWEEK(chicago_taxi.TripStart)
from chicago_taxi_prediction_4_40
join chicago_taxi on chicago_taxi.TripID = chicago_taxi_prediction_4_40.TripID
group by 2
order by 2;
  
  
-- analyzing model: group by company
select 
  SQRT(sum((chicago_taxi_prediction_4_40.fare - prediction) * (chicago_taxi_prediction_4_40.fare - prediction)) / count(*)) as RMSE,
  chicago_taxi.company
from chicago_taxi_prediction_4_40
join chicago_taxi on chicago_taxi.TripID = chicago_taxi_prediction_4_40.TripID
group by 2
order by 1;


-------------  Prediction  --------------
---- Whole pipeline, except for no "fare" column
