DROP TABLE IF EXISTS MART.Fact_Flights;
CREATE TABLE IF NOT EXISTS MART.Fact_Flights(
AIRPORT	string
,AIRLINE	string
,CANCELLATION_REASON	string
,Delay_Reason	string
,No_of_flights	bigint
,Total_Percentage	double
,Most_Unique_Route	int
,Is_largest_delay	int
,Is_Cancel_Reason	int
,Is_Delay	int
,Year	int
,Month	int
 );

DROP TABLE IF EXISTS MART.Flights;
CREATE TABLE IF NOT EXISTS MART.Flights(
YEAR integer
,MONTH integer
,DAY integer
,DAY_OF_WEEK integer
,AIRLINE string
,FLIGHT_NUMBER integer
,TAIL_NUMBER string
,ORIGIN_AIRPORT string
,DESTINATION_AIRPORT string
,SCHEDULED_DEPARTURE integer
,DEPARTURE_TIME String
,DEPARTURE_DELAY String
,TAXI_OUT string
,WHEELS_OFF integer
,SCHEDULED_TIME integer
,ELAPSED_TIME integer
,AIR_TIME integer
,DISTANCE integer
,WHEELS_ON integer
,TAXI_IN string
,SCHEDULED_ARRIVAL integer
,ARRIVAL_TIME integer
,ARRIVAL_DELAY integer
,DIVERTED integer
,CANCELLED integer
,CANCELLATION_REASON string
,AIR_SYSTEM_DELAY integer
,SECURITY_DELAY integer
,AIRLINE_DELAY integer
,LATE_AIRCRAFT_DELAY integer
,WEATHER_DELAY integer
);
 DROP TABLE IF EXISTS MART.Airlines;
 CREATE TABLE IF NOT EXISTS MART.Airlines
 (
 IATA_CODE string,
 AIRLINE string
  );
  DROP TABLE IF EXISTS MART.Airport;
 CREATE TABLE IF NOT EXISTS MART.Airport
 (
 IATA_CODE string
,AIRPORT string
,CITY string
,STATE string
,COUNTRY string
,LATITUDE double
,LONGITUDE double
  );


COPY INTO MART.Airlines
FROM @azure_stage pattern='airlines.csv' file_format = (type=CSV SKIP_HEADER=1)


--COPY INTO MART.Fact_Flights
--FROM @azure_stage/FACT_Flight pattern='.*.csv';

COPY INTO MART.Flights
FROM @azure_stage pattern='partition-[0-9]*.csv' file_format = (type=CSV SKIP_HEADER=1)


COPY INTO MART.Airport
FROM @azure_stage pattern='airports.csv' file_format = (type=CSV SKIP_HEADER=1);

DROP MATERIALIZED VIEW IF EXISTS Mart.VW_TOTAL_FLIGHTS;
CREATE  MATERIALIZED VIEW Mart.VW_TOTAL_FLIGHTS
as
select * from "USER_SRUTHI"."MART"."FLIGHTS_MONTHLY"


DROP MATERIALIZED VIEW IF EXISTS Mart.VW_Most_Unique;
CREATE  MATERIALIZED VIEW Mart.VW_Most_Unique
as
select * from "USER_SRUTHI"."MART"."AIRLINE_UNIQUEROUTES"


DROP MATERIALIZED VIEW IF EXISTS Mart.VW_Largest_Delay;
CREATE  MATERIALIZED VIEW Mart.VW_Largest_Delay
as
select * from "USER_SRUTHI"."MART"."AIRLINE_LARGESTNOOFDELAY"


DROP MATERIALIZED VIEW IF EXISTS Mart.VW_Percentage_On_Time;
CREATE  MATERIALIZED VIEW Mart.VW_Percentage_On_Time
as
select * from "USER_SRUTHI"."MART"."AIRLINE_PERCENTAGE_ONTIME"


DROP MATERIALIZED VIEW IF EXISTS Mart.VW_Cancel_Reason;
CREATE  MATERIALIZED VIEW Mart.VW_Cancel_Reason
as
select * from "USER_SRUTHI"."MART"."CANCELATION_AIRPORT"



DROP MATERIALIZED VIEW IF EXISTS Mart.VW_Delay_Reason;
CREATE  MATERIALIZED VIEW Mart.VW_Delay_Reason
as
select * from "USER_SRUTHI"."MART"."DELAYREASON_AIRPORT"
