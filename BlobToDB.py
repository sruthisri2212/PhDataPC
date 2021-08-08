# Databricks notebook source
# MAGIC %md 
# MAGIC ##Read configuration details

# COMMAND ----------

#dbutils.fs.cp("/FileStore/tables/jobConfig-3.conf", "file:///tmp/config.ini")
import configparser
config = configparser.ConfigParser()
config.read("/tmp/config.ini")
sfusername=config.get("CREDENTIALS", "sfusername")
sfpassword=config.get("CREDENTIALS", "sfpassword")

# COMMAND ----------

# MAGIC %md ##Mount Blob storage in Databricks

# COMMAND ----------

# MAGIC %scala
# MAGIC //spark.conf.set(
# MAGIC //  "fs.azure.sas.adb.snowflakeblob.blob.core.windows.net",
# MAGIC //  "xxxxxxxxxxx")
# MAGIC //#Or we have option to mount blob storage and read data
# MAGIC 
# MAGIC val containerName = "adb"
# MAGIC val storageAccountName = "snowflakeblob"
# MAGIC val sas = "xxxxxx"
# MAGIC val config = "fs.azure.sas." + containerName+ "." + storageAccountName + ".blob.core.windows.net"
# MAGIC 
# MAGIC dbutils.fs.mount(
# MAGIC source = "wasbs://adb@snowflakeblob.blob.core.windows.net/",
# MAGIC mountPoint = "/mnt/storage",
# MAGIC extraConfigs = Map(config -> sas))

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Reading csv files from BlobStorage into Dataframes and creating tempview in spark

# COMMAND ----------

#path_blob="wasbs://adb@snowflakeblob.blob.core.windows.net/"
airports_file_location = "/mnt/storage/"+"airports.csv"
flight_file_location = "/mnt/storage/"+"partition-*.csv"
airlines_file_location = "/mnt/storage/"+"airlines.csv"

file_type = "csv"
infer_schema = "True"
first_row_is_header = "True"
delimiter = ","

airportdf=spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.option("header", first_row_is_header) \
.option("sep", delimiter) \
.load(airports_file_location)
airportdf.dropDuplicates().cache().createOrReplaceTempView('airports')
#df.write.mode('overwrite').saveAsTable('stage.airports')

airlinesdf=spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.option("header", first_row_is_header) \
.option("sep", delimiter) \
.load(airlines_file_location)
airlinesdf.dropDuplicates().cache().createOrReplaceTempView('airlines')
#df.write.mode('overwrite').saveAsTable('stage.airlines')
#airlinesDf.createOrReplaceTempView('airlines')


flightsdf=spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.option("header", first_row_is_header) \
.option("sep", delimiter) \
.load(flight_file_location)
flightsdf.dropDuplicates().cache().createOrReplaceTempView('flights')
#df.write.mode('overwrite').saveAsTable('stage.flights')
#spark.catalog.dropTempView("flights") delete view if already exists
#flightsDf.createOrReplaceTempView('flights')

# COMMAND ----------

# MAGIC %md ##Data count  

# COMMAND ----------

print("airports data count----->",spark.sql("select * from airports").count())
print("airlines data count----->",spark.sql("select * from airlines").count())
print("flights data count----->",spark.sql("select * from flights").count())
spark.sql("select * from airlines").printSchema()
spark.sql("select * from airports").printSchema()

# COMMAND ----------

# MAGIC %md #Reports

# COMMAND ----------

# MAGIC %md ##Report 1 - Total number of flights by airline and airport on a monthly basis 

# COMMAND ----------

display(spark.sql("""
select IFNULL(AL.AIRLINE, 'Unknown') as AIRLINE, IFNULL(ap.AIRPORT, 'Unknown') AS AIRPORT
  ,Year,Month,
 count( distinct FLIGHT_NUMBER) AS No_of_flights
 from flights f LEFT JOIN airlines AL 
 ON trim(F.AIRLINE) = trim(AL.IATA_CODE)
 left join airports ap 
 on trim(F.ORIGIN_AIRPORT) = trim(ap.IATA_CODE) 
 group by AL.AIRLINE,ap.AIRPORT,Year,Month  ORDER BY AIRLINE,month"""))

# COMMAND ----------

# MAGIC %md ##Report 2 - On time percentage of each airline for the year 2015

# COMMAND ----------

display(spark.sql("""with cte as 
(
select IFNULL(AL.AIRLINE, 'Unknown') AIRLINE ,FLIGHT_NUMBER,
CASE WHEN IFNULL(DEPARTURE_DELAY,0)=0 THEN 1 ELSE 0 END as nodelay
    from flights f LEFT JOIN airlines AL 
    ON trim(F.AIRLINE) = trim(AL.IATA_CODE) where year =2015 
    ) 

select AIRLINE,(SUM(nodelay)/count(FLIGHT_NUMBER))*100 Total_Percentage from cte GROUP BY AIRLINE  order by Total_Percentage desc"""))


# COMMAND ----------

# MAGIC %md ##Report 3 - Airlines with the largest number of delays

# COMMAND ----------

display(spark.sql("""
with cte as (
select IFNULL(AL.AIRLINE, 'Unknown') AIRLINE,
CASE WHEN IFNULL(ARRIVAL_DELAY,0) <> 0  THEN 1 ELSE 0 END as arrivaldelay 
from flights f LEFT JOIN airlines AL  ON trim(F.AIRLINE) = trim(AL.IATA_CODE) 
)
select AIRLINE,largestdelay from (select AIRLINE,SUM(arrivaldelay) as largestdelay from cte group by airline) order by largestdelay desc limit 1"""))


# COMMAND ----------

# MAGIC %md ## Report 4 - Cancellation reasons by airport

# COMMAND ----------

display(spark.sql("""
select distinct
    IFNULL(ap.AIRPORT, 'Unknown') AS AIRPORT,
     SUM(CASE WHEN CANCELLATION_REASON ='A' THEN 1 ELSE 0 END) AS AirlineCarrier,
   SUM(CASE WHEN CANCELLATION_REASON ='B' THEN 1 ELSE 0 END) AS Weather,
   SUM(CASE WHEN CANCELLATION_REASON ='C' THEN 1 ELSE 0 END) AS NationalAirSystem,
   SUM(CASE WHEN CANCELLATION_REASON ='D' THEN 1 ELSE 0 END) AS Security
    from flights f 
   left join airports ap 
   on trim(F.ORIGIN_AIRPORT) = trim(ap.IATA_CODE) where CANCELLED=1
GROUP BY AIRPORT
ORDER BY AIRPORT """))

# COMMAND ----------

# MAGIC %md ## Report 5 - Delay reasons by airport

# COMMAND ----------

display(spark.sql("""
 with cte as ( 
 select IFNULL(ap.AIRPORT, 'Unknown') AS AIRPORT,
    sum(case WHEN ifnull(AIRLINE_DELAY,0) > 0 THEN 1 else 0 end) as airlinedelay,
     sum(case WHEN ifnull(SECURITY_DELAY,0) > 0 THEN 1 else 0 end) as securitydelay,
      sum(case WHEN ifnull(AIR_SYSTEM_DELAY,0) > 0 THEN 1 else 0 end) as airsystemdelay,
       sum(case WHEN ifnull(LATE_AIRCRAFT_DELAY,0) > 0 THEN 1 else 0 end) as lateaircraftdelay
from flights f 
   left join airports ap on trim(F.ORIGIN_AIRPORT) = trim(ap.IATA_CODE) group by airport 
   )

select distinct * from cte """))

# COMMAND ----------

# MAGIC %md ## Report 6 - Airline with the most unique routes

# COMMAND ----------

display(spark.sql("""
with temp as (select IFNULL(AL.AIRLINE, 'Unknown') as AIRLINE,ORIGIN_AIRPORT,DESTINATION_AIRPORT,count(*) as cnt
      from flights f
      LEFT JOIN AIRLINES AL ON trim(F.AIRLINE) = trim(AL.IATA_CODE)  
      group by AL.AIRLINE,ORIGIN_AIRPORT,DESTINATION_AIRPORT)

select AIRLINE,sum(cnt) as uniqueroutes from temp where cnt=1 group by AIRLINE order by sum(cnt) desc limit 1"""))

# COMMAND ----------

# MAGIC %md # Snowflake connection

# COMMAND ----------

options = {
  "sfUrl": "https://tf38567.central-us.azure.snowflakecomputing.com",
  "sfUser": config.get("CREDENTIALS", "sfusername"),
  "sfPassword": config.get("CREDENTIALS", "sfpassword"),
  "sfDatabase": "USER_SRUTHI",
  "sfSchema": "DEMO",
 "sfWarehouse": "INTERVIEW_WH"
}

# COMMAND ----------

# MAGIC %md ##Loading dataframes into snowflake tables

# COMMAND ----------


airportdf.write.format("snowflake").options(**options).option("dbtable", "AIRPORT").mode("OVERWRITE").save()
airlinesdf.write.format("snowflake").options(**options).option("dbtable", "AIRLINES").mode("OVERWRITE").save()
flightsdf.write.format("snowflake").options(**options).option("dbtable", "FLIGHTS").mode("OVERWRITE").save()

# COMMAND ----------

# MAGIC %md ##Write Reports to Snowflake

# COMMAND ----------


df1=spark.sql("""
select IFNULL(AL.AIRLINE, 'Unknown') as AIRLINE, IFNULL(ap.AIRPORT, 'Unknown') AS AIRPORT
  ,Year,Month,
 count( distinct FLIGHT_NUMBER) AS No_of_flights
 from flights f LEFT JOIN airlines AL 
 ON trim(F.AIRLINE) = trim(AL.IATA_CODE)
 left join airports ap 
 on trim(F.ORIGIN_AIRPORT) = trim(ap.IATA_CODE) 
 group by AL.AIRLINE,ap.AIRPORT,Year,Month  ORDER BY AIRLINE,month""")

df1.write.format("snowflake").options(**options).option("dbtable", "FLIGHTS_MONTHLY").mode("OVERWRITE").save()

# COMMAND ----------


df2=spark.sql("""with cte as 
(
select IFNULL(AL.AIRLINE, 'Unknown') AIRLINE ,FLIGHT_NUMBER,
CASE WHEN IFNULL(DEPARTURE_DELAY,0)=0 THEN 1 ELSE 0 END as nodelay
    from flights f LEFT JOIN airlines AL 
    ON trim(F.AIRLINE) = trim(AL.IATA_CODE) where year =2015 
    ) 

select AIRLINE,(SUM(nodelay)/count(FLIGHT_NUMBER)*1.0)*100 Total_Percentage from cte GROUP BY AIRLINE  order by Total_Percentage desc""")

df2.write.format("snowflake").options(**options).option("dbtable", "AIRLINE_PERCENTAGE_ONTIME").mode("OVERWRITE").save()

# COMMAND ----------

df3=spark.sql("""
with cte as (
select IFNULL(AL.AIRLINE, 'Unknown') AIRLINE,
CASE WHEN IFNULL(ARRIVAL_DELAY,0) <> 0  THEN 1 ELSE 0 END as arrivaldelay 
from flights f LEFT JOIN airlines AL  ON trim(F.AIRLINE) = trim(AL.IATA_CODE) 
)
select AIRLINE,largestdelay from (select AIRLINE,SUM(arrivaldelay) as largestdelay from cte group by airline) order by largestdelay desc limit 1""")

df3.write.format("snowflake").options(**options).option("dbtable", "AIRLINE_LARGESTNOOFDELAY").mode("OVERWRITE").save()

# COMMAND ----------

df4=spark.sql("""
select distinct
    IFNULL(ap.AIRPORT, 'Unknown') AS AIRPORT,
     SUM(CASE WHEN CANCELLATION_REASON ='A' THEN 1 ELSE 0 END) AS AirlineCarrier,
   SUM(CASE WHEN CANCELLATION_REASON ='B' THEN 1 ELSE 0 END) AS Weather,
   SUM(CASE WHEN CANCELLATION_REASON ='C' THEN 1 ELSE 0 END) AS NationalAirSystem,
   SUM(CASE WHEN CANCELLATION_REASON ='D' THEN 1 ELSE 0 END) AS Security
    from flights f 
   left join airports ap 
   on trim(F.ORIGIN_AIRPORT) = trim(ap.IATA_CODE) where CANCELLED=1
GROUP BY AIRPORT
ORDER BY AIRPORT """)

df4.write.format("snowflake").options(**options).option("dbtable", "CANCELATION_AIRPORT").mode("OVERWRITE").save()

# COMMAND ----------

df5=spark.sql("""
 with cte as ( 
 select IFNULL(ap.AIRPORT, 'Unknown') AS AIRPORT,
    sum(case WHEN ifnull(AIRLINE_DELAY,0) > 0 THEN 1 else 0 end) as airlinedelay,
     sum(case WHEN ifnull(SECURITY_DELAY,0) > 0 THEN 1 else 0 end) as securitydelay,
      sum(case WHEN ifnull(AIR_SYSTEM_DELAY,0) > 0 THEN 1 else 0 end) as airsystemdelay,
       sum(case WHEN ifnull(LATE_AIRCRAFT_DELAY,0) > 0 THEN 1 else 0 end) as lateaircraftdelay
from flights f 
   left join airports ap on trim(F.ORIGIN_AIRPORT) = trim(ap.IATA_CODE) group by airport 
   )

select distinct * from cte """)

df5.write.format("snowflake").options(**options).option("dbtable", "DELAYREASON_AIRPORT").mode("OVERWRITE").save()

# COMMAND ----------

df6=spark.sql("""
with temp as (select IFNULL(AL.AIRLINE, 'Unknown') as AIRLINE,ORIGIN_AIRPORT,DESTINATION_AIRPORT,count(*) as cnt
      from flights f
      LEFT JOIN AIRLINES AL ON trim(F.AIRLINE) = trim(AL.IATA_CODE)  
      group by AL.AIRLINE,ORIGIN_AIRPORT,DESTINATION_AIRPORT)

select AIRLINE,sum(cnt) as uniqueroutes from temp where cnt=1 group by AIRLINE order by sum(cnt) desc limit 1""")

df6.write.format("snowflake").options(**options).option("dbtable", "AIRLINE_UNIQUEROUTES").mode("OVERWRITE").save()

# COMMAND ----------


