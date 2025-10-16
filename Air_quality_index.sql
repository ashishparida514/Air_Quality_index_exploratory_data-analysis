 /*  1.Average AQI by city (daily)
 For a given date range, compute daily average AQI for each city  */


select City , round(avg(AQI) ,3) as daily_average_AQI
from `Air_quality_index.city_day`
where AQI is not null
group by City
order by daily_average_AQI asc


  

/*  2.Hourly average pollutant levels for a station
For station AP001 and date range (October to december), compute average PM2_5, PM10, etc. for each hour of day (0–23).  */

select extract(hour from Datetime) as hour_of_day , round(avg(PM2_5),2) as average_PM_5 ,
       round(avg(PM10),2) as average_PM10 ,round(avg(NO2),2) as average_NO2 ,round(avg(NOx),2) as average_NOx,
       round(avg(NH3),2) as average_NH3 , round(avg(CO),2) as average_CO ,round(avg(SO2),2) as average_SO2 , 
       round(avg(O3),2) as average_O3 ,round(avg(Benzene),2) as average_Benzene , round(avg(Toluene),2) as average_Toluene, 
       round(avg(Xylene),2) as average_Xylene ,round(avg(AQI),2) as average_AQI 
from `Air_quality_index.station_hour`
where  extract(date from Datetime) between '2019-10-01' and '2019-10-31'  and StationId = 'AP005'
group by extract(hour from Datetime)


  
/*  3.Station-level worst day
 For each station, find the day (from station_day) with the highest daily AQI, and report that day and AQI.  */

select Station_Name ,Date ,AQI
from (
      select Station_Name,Date ,A.AQI ,dense_rank() over (partition by A.StationId order by A.AQI desc) as highest_of_day
      from `Air_quality_index.station_day` A
      inner join `Air_quality_index.stations` B
      on A.StationId = B.station_id ) a
where highest_of_day = 1 and AQI is not null
order by AQI desc


  
/*  4.Most frequent AQI bucket per station
  For each station, count how many days  fell into each AQI_Bucket (Good, Moderate, etc.), and show the bucket which appears most. */

select Station_Name , AQI_Bucket , count(*) as count_AQI_bucket
from `Air_quality_index.station_day` A
join `Air_quality_index.stations` B
on A.StationId = B.station_id
where AQI_Bucket is not null
group by Station_Name , AQI_Bucket
order by count_AQI_bucket desc


/* 5.Top 10 most polluted cities (yearly)
For a 2019 year, rank the top 10 cities by their annual average AQI.  */

select city, average_AQI
from ( 
      select city, round(avg(AQI), 2) as average_AQI, dense_rank() over (order by AVG(AQI) desc) as rnk
      from `Air_quality_index.city_day`
      where Date between '2019-01-01' and '2019-12-31'
      group by  city ) a
where rnk >= 10


  
/* 6.Monthly trend of PM2_5 for a city
For a (bengaluru) city , compute the monthly average PM2_5 for all months in a given year, in order, to see the trend. */

select year , month ,round( average_PM2_5 - lag(average_PM2_5) over (order by year , month) ,2) as monthly_trend
from (
       select extract(year from date ) as year , extract(month from date) as month , round(avg(PM2_5),2) as average_PM2_5
       from `Air_quality_index.city_day`
       where City = 'Bengaluru' and PM2_5 is not null
       group by extract(year from date) , extract(month from date) ) a
where average_PM2_5 is not null


  
/* 7.	Station with most active status
Count readings (days) per station, and list the top few stations with the most data (highest count). */


select s.Station_id, s.Station_Name, s.City, s.State, s.Status,
count(*) as reading_count
from `Air_quality_index.stations` as s
join `Air_quality_index.station_day` as r
on s.Station_id = r.StationId
group by  s.Station_id, s.Station_Name, s.City, s.State, s.Status
order by reading_count desc
limit 10




/* 8.Before vs after policy
Suppose a regulation began on 2019-01-01. For delhi city, compare average AQI and pollutant levels before vs after in given time windows. */

select A.year,B.year , round((average_AQI_2019 - average_AQI_2018),2) as difference_AQI , 
      round((average_PM2_5_2019 - average_PM2_5_2018),2) as difference_PM2_5,
      round(( average_PM10_2019 - average_PM10_2018 ),2) as difference_PM10 ,
      round((average_NOx_2019 - average_NOx_2018 ),2) as difference_NOx ,
      round((average_CO_2019 - average_CO_2018 ),2) as difference_CO ,
      round((average_SO2_2019 - average_SO2_2018 ),2) as difference_SO2 ,
      round((average_O3_2019 - average_O3_2018 ),2) as difference_O3 
from
     (select City , extract(year from date) as year,round(avg(AQI),2) as average_AQI_2018 , round(avg(PM2_5),2) as average_PM2_5_2018 , 
             round(avg(PM10),2) as average_PM10_2018 ,  round(avg(NOx),2) as average_NOx_2018 , round(avg(CO),2) as average_CO_2018 , 
             round(avg(SO2),2) as average_SO2_2018 ,round(avg(O3),2) as average_O3_2018 
      from `Air_quality_index.city_day`
      where City = 'Delhi' and date between '2018-01-01' and '2018-12-31'
      group by City , extract(year from date) ) A
join 
     (select City , extract(year from date) as year,round(avg(AQI),2) as average_AQI_2019 , round(avg(PM2_5),2) as average_PM2_5_2019 , 
             round(avg(PM10),2) as average_PM10_2019 ,  round(avg(NOx),2) as average_NOx_2019 , round(avg(CO),2) as average_CO_2019 , 
             round(avg(SO2),2) as average_SO2_2019 ,round(avg(O3),2) as average_O3_2019 
      from `Air_quality_index.city_day`
      where City = 'Delhi' and date between '2019-01-01' and '2019-12-31'
      group by City ,extract(year from date) ) B
on A.City = B.City



  
/*  9.Day-of-week effect
In a city(chennai), compute average AQI by day of week (Monday, Tuesday, …) to see which days typically have worse air. */

select date , format_datetime('%A' , date) as day_of_week , average_AQI
from (
     select date , avg(AQI) as average_AQI 
     from `Air_quality_index.city_day` 
     where City = 'Chennai' and AQI is not null
     group by date ) a 
order by average_AQI desc


  
  /* 10.	Hourly peaks
For each station, find the hour (0–23) at which average AQI is highest. */

select station_name , hour_of_day , round(average_AQI,2) as average_AQI
from (
      select station_id,Station_Name , extract(hour from Datetime) as hour_of_day , avg(AQI) as average_AQI ,
      dense_rank() over ( partition by station_id order by avg(AQI) desc) as rnk
      from `Air_quality_index.station_hour` A
      join `Air_quality_index.stations` B
      on A.StationId = B.station_id
      where AQI is not null
      group by station_id , Station_Name , extract(hour from Datetime)
      order by average_AQI desc ) a 
where rnk = 1 
order by average_AQI desc


  
 /* 11.	Consecutive high pollution days
For a station, find sequences of days where AQI was “Severe” (or above a threshold) for 3 consecutive days. */


select station_id ,Station_Name , Date , AQI_Bucket , prev_date , next_date
from (
      select B.station_id , B.Station_Name , A.Date , A.AQI_Bucket , 
             lag(A.Date) over (partition by A.StationId order by A.Date) as  prev_date ,
             lead(A.Date) over (partition by A.StationId order by A.Date) as next_date,
             lag(AQI_Bucket) over (partition by StationId order by A.Date) as prev_bucket,
            lead(AQI_Bucket) over (partition by StationId order by A.Date) as next_bucket
      From `Air_quality_index.station_day` A
      join `Air_quality_index.stations` B
      on A.StationId = B.station_id )a 
where AQI_Bucket = 'Severe' and prev_bucket = 'Severe' and next_bucket = 'Severe' and 
      datetime_diff(Date , prev_date,day) = 1 and 
      datetime_diff(next_date ,Date , day) = 1

  

/* 12.	Transition analysis
 For station or city: how many times did AQI category go from “Moderate” to “Severe” (or jump two categories) day to day. */

  
select City , count(1) as moderate_to_severe_count
from (
      select City , Date , AQI_Bucket ,
             lead(Date) over (partition by City order by Date) as next_date , 
             lead(AQI_Bucket) over (partition by City order by Date ) as AQI_bucket_next
      from `Air_quality_index.city_day` ) A
      where AQI_Bucket = 'Moderate' and AQI_Bucket_next = 'Severe'and
      date_diff(next_date ,Date ,DAY ) = 1 
group by City


  

/* 13.	List severe AQI events
 List all station-day records where AQI is above a threshold (e.g. “Severe” bucket). Include station, city, datetime,  pollutant, etc. */


select StationId , Station_Name , City , Date , AQI_Bucket , AQI, PM2_5 , PM10 , NOx ,CO ,O3 ,SO2
from `Air_quality_index.station_day` A
join `Air_quality_index.stations` B
on A.StationId = B.station_id
where AQI_Bucket = 'Severe' 


  /*14.	Minimum pollutant values
 For each pollutant (PM2_5, PM10, NO2, etc.), find the minimum non-null value recorded in 2020 year, and which station  it occurred. */


select StationId , Station_Name , City , Date , 
       min(AQI) as min_value_AQI, min(PM2_5) as min_value_PM2_5 , 
       min(PM10) as min_value_PM10 , min(NO2) as min_value_NO2 ,min(CO) as min_value_CO 
      ,min(O3) as min_value_O3 ,min(SO2) as min_value_SO2
from `Air_quality_index.station_day` A
join `Air_quality_index.stations` B
on A.StationId = B.station_id
where Date between '2020-01-01' and '2020-12-31' and A.PM2_5 is not null
      and A.PM10 is not null  and A.NO2 is not null and A.CO is not null  
      and A.O3 is not null and A.SO2 is not null
group by StationId , Station_Name , City ,Date



/*  15.Missing data detection
 For each station and month, count how many hours have NULL in any pollutant column.  */



select StationId , Station_Name ,format_datetime('%b' , Date) as month , sum(case when PM2_5 is null then 1 else 0 end) as null_count_of_PM2_5 ,
       sum(case when PM10 is null then 1 else 0 end ) as null_count_of_PM10 ,
       sum(case when NO2 is null then 1 else 0 end ) as null_count_of_NO2,
       sum(case when CO is null then 1 else 0 end) as null_count_of_CO , 
       sum(case when SO2 is null then 1 else 0 end ) as null_count_of_SO2
from `Air_quality_index.station_day` A
join `Air_quality_index.stations` B
on A.StationId = B.station_id
group by StationId , Station_Name ,format_datetime('%b' ,date) 
