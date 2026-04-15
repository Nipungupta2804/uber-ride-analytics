-- ================================================
-- ================================================
-- UBER RIDES DATA - PROJECT 
-- ================================================
-- ================================================


-- =========================================
-- DATABASE SETUP
-- =========================================
CREATE DATABASE uber_project;
USE uber_project;

show variables like 'local_infile';
set global local_infile = 1;
select @@default_storage_engine;
SHOW VARIABLES LIKE 'secure_file_priv';


-- =========================================
-- DATA INGESTION
-- =========================================
# Rides
CREATE TABLE rides (
    Booking_ID VARCHAR(20),
    Booking_Timestamp DATETIME,
    Customer_ID VARCHAR(20),
    Driver_ID VARCHAR(20),
    Vehicle_Type VARCHAR(50),
    Pickup_Location VARCHAR(100),
    Drop_Location VARCHAR(100),
    Ride_Distance_KM DECIMAL(8,2),
    Payment_Method VARCHAR(50),
    Peak_Hour_Flag INT,
    Surge_Multiplier DECIMAL(10,2),
    Estimated_Ride_Min INT,
    Actual_Ride_Min INT,
    Estimated_Fare DECIMAL(10,2),
    Booking_Status VARCHAR(20),
    Final_Paid_Amount DECIMAL(10,3)
    ) ENGINE=InnoDB;
	
LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Rides.csv'
INTO TABLE rides
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
Booking_ID,
Booking_Timestamp,
Customer_ID,
Driver_ID,
Vehicle_Type,
Pickup_Location,
Drop_Location,
@Ride_Distance_KM,
Payment_Method,
Peak_Hour_Flag,
Surge_Multiplier,
@Estimated_Ride_Min,
@Actual_Ride_Min,
@Estimated_Fare,
Booking_Status,
@Final_Paid_Amount
)
SET
Ride_Distance_KM = NULLIF(@Ride_Distance_KM, ''),
Estimated_Ride_Min = NULLIF(@Estimated_Ride_Min, ''),
Actual_Ride_Min = NULLIF(@Actual_Ride_Min, ''),
Estimated_Fare = NULLIF(@Estimated_Fare, ''),
Final_Paid_Amount = NULLIF(trim(@Final_Paid_Amount), '');

select count(*) from rides;



# Ride Status
CREATE TABLE ride_status (
    Booking_ID VARCHAR(20),
    Cancelled_By_Customer INT,
    Cancelled_By_Driver INT,
    Cancellation_Reason VARCHAR(255),
    Incomplete_Reason VARCHAR(255)) ENGINE=InnoDB;

LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/ride_status.csv'
INTO TABLE ride_status
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS(Booking_ID,
Cancelled_By_Customer,
Cancelled_By_Driver,
Cancellation_Reason,
Incomplete_Reason);
select count(*) from ride_status;



# Customers
CREATE TABLE customers (
    Customer_ID VARCHAR(20),
    Customer_Signup_Date DATETIME,
    Customer_City VARCHAR(100)) ENGINE=InnoDB;

LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/customers.csv'
INTO TABLE customers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS(
Customer_ID,
@Customer_Signup_Date,
Customer_City)
SET Customer_Signup_Date = STR_TO_DATE(@Customer_Signup_Date, '%Y-%m-%d %H:%i:%s');
select count(*) from customers;



# Drivers
CREATE TABLE drivers (
    Driver_ID VARCHAR(20),
    Driver_Joining_Date DATE,
    Driver_Experience_Years INT,
    Driver_Base_City VARCHAR(100)) ENGINE=InnoDB;

LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/drivers.csv'
INTO TABLE drivers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS(Driver_ID,
@Driver_Joining_Date,
Driver_Experience_Years,
Driver_Base_City)SET Driver_Joining_Date = STR_TO_DATE(@Driver_Joining_Date, '%Y-%m-%d');
select count(*) from drivers;



# Vehicles
CREATE TABLE vehicles (
    Vehicle_Type VARCHAR(50),
    Base_Fare DECIMAL(10,2),
    Per_KM_Rate DECIMAL(10,2)) ENGINE=InnoDB;

LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/vehicles.csv'
INTO TABLE vehicles
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS(Vehicle_Type,
Base_Fare,
Per_KM_Rate);
select count(*) from vehicles;

-- =========================================
-- DATA CLEANING
-- =========================================
-- => Duplicates Handling
DELIMITER \\
CREATE PROCEDURE CD(IN t VARCHAR(50),IN c VARCHAR(50))
BEGIN
    SET @sql = CONCAT(
	'SELECT COUNT(*) AS Duplicate_Cnt ','FROM ( ',
	'SELECT ', c, ', COUNT(', c, ') AS cnt ',
	'FROM ', t, ' ','GROUP BY ', c, ' ',
	'HAVING COUNT(', c, ') > 1 ',') x');
	PREPARE stmt FROM @sql;
	EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END \\
DELIMITER ;

call CD('rides','booking_id');
call CD('drivers','driver_id');
call CD('customers','customer_id');
call CD('ride_status','booking_id');
call CD('vehicles','vehicle_type');

-- => Dropping Duplicates
CREATE TABLE rides_clean AS
SELECT * FROM 
(SELECT *,ROW_NUMBER() OVER (
PARTITION BY Booking_ID ORDER BY Booking_Timestamp DESC) AS rn
FROM rides) t
WHERE rn = 1;

CREATE TABLE ridec AS 
SELECT * FROM rides;                # Creating backup ridec(Ride Copy)

DROP TABLE rides;                   # Dropping uncleaned Table
 
RENAME TABLE rides_clean to rides;  # Renaming Cleaned Table

Call CD('rides','Booking_ID');      # Rechecking Duplicates


-- =========================================
-- NULL VALUES AUDIT
-- =========================================
-- => Checking Null
SELECT 'Customer_ID' AS Column_Name, COUNT(*) AS Null_Count FROM rides WHERE Customer_ID IS NULL
UNION ALL
SELECT 'Driver_ID', COUNT(*) FROM rides WHERE Driver_ID IS NULL
UNION ALL
SELECT 'Vehicle_Type', COUNT(*) FROM rides WHERE Vehicle_Type IS NULL
UNION ALL
SELECT 'Pickup_Location', COUNT(*) FROM rides WHERE Pickup_Location IS NULL
UNION ALL
SELECT 'Booking_Timestamp', COUNT(*) FROM rides WHERE Booking_Timestamp IS NULL
UNION ALL
SELECT 'Drop_Location', COUNT(*) FROM rides WHERE Drop_Location IS NULL
UNION ALL
SELECT 'Ride_Distance_KM', COUNT(*) FROM rides WHERE Ride_Distance_KM IS NULL
UNION ALL
SELECT 'Actual_Ride_Min', COUNT(*) FROM rides WHERE Actual_Ride_Min IS NULL
UNION ALL
SELECT 'Estimated_Fare', COUNT(*) FROM rides WHERE Estimated_Fare IS NULL
UNION ALL
SELECT 'Booking_Status', COUNT(*) FROM rides WHERE Booking_Status IS NULL
UNION ALL
SELECT 'Final_paid_Amount', COUNT(*) FROM rides WHERE Final_paid_Amount IS NULL
UNION ALL
SELECT 'Surge_Multiplier', COUNT(*) FROM rides WHERE Surge_Multiplier IS NULL
UNION ALL
SELECT 'Estimated_Ride_Min', COUNT(*) FROM rides WHERE Estimated_Ride_Min IS NULL;



SELECT 'Customer_ID' AS Column_Name, COUNT(*) AS Null_Count FROM customers WHERE Customer_ID IS NULL
UNION ALL
SELECT 'Customer_City', COUNT(*) FROM customers WHERE Customer_City IS NULL
UNION ALL
SELECT 'Customer_Signup_Date', COUNT(*) FROM customers WHERE Customer_Signup_Date IS NULL;

SELECT 'Driver_ID' AS Column_Name, COUNT(*) AS Null_Count FROM drivers WHERE Driver_ID IS NULL
UNION ALL
SELECT 'Driver_Joining_Date', COUNT(*) FROM drivers WHERE Driver_Joining_Date IS NULL
UNION ALL
SELECT 'Driver_Experience_Years', COUNT(*) FROM drivers WHERE Driver_Experience_Years IS NULL
UNION ALL
SELECT 'Driver_Base_City', COUNT(*) FROM drivers WHERE Driver_Base_City IS NULL;

SELECT 'Vehicle_Type' AS Column_Name, COUNT(*) AS Null_Count FROM vehicles WHERE Vehicle_Type IS NULL
UNION ALL
SELECT 'Base_Fare', COUNT(*) FROM vehicles WHERE Base_Fare IS NULL
UNION ALL
SELECT 'Per_KM_Rate', COUNT(*) FROM vehicles WHERE Per_KM_Rate IS NULL;

SELECT 'Booking_ID' AS Column_Name, COUNT(*) AS Null_Count FROM ride_status WHERE Booking_ID IS NULL
UNION ALL
SELECT 'Cancelled_By_Customer', COUNT(*) FROM ride_status WHERE Cancelled_By_Customer IS NULL
UNION ALL
SELECT 'Cancelled_By_Driver', COUNT(*) FROM ride_status WHERE Cancelled_By_Driver IS NULL
UNION ALL
SELECT 'Cancellation_Reason', COUNT(*) FROM ride_status WHERE Cancellation_Reason IS NULL
UNION ALL
SELECT 'Incomplete_Reason', COUNT(*) FROM ride_status WHERE Incomplete_Reason IS NULL;


-- => Null Value Handling 
set sql_safe_updates = 0;
### => Rides Table
Update rides set Pickup_location = 'Unknown'
where Pickup_location IS null;
Update rides set Drop_location = 'Unknown'
where Drop_location IS null;

Update rides set Ride_Distance_KM = 
(Select val from
(select avg(Ride_Distance_KM) val from rides)t)
where Ride_Distance_KM IS NULL;

update rides set payment_method = 'Unknown'
where payment_method IS null;

update rides set Actual_Ride_Min = Estimated_Ride_Min
where Actual_Ride_Min IS null and Booking_Status = 'Completed';
update rides set Actual_Ride_Min = 0
where Actual_Ride_Min IS null and Booking_Status in ('Incomplete','Cancelled');

update customers set customer_city = 'Unknown'
where customer_city IS null;

update ride_status set cancellation_reason = 'Not Applicable'
where cancellation_reason IS null;

update ride_status set Incomplete_reason = 'Not Applicable'
where Incomplete_reason IS null;



-- =========================================
-- BUSINESS RULE VALIDATION
-- =========================================
update rides set Final_Paid_Amount = 0
where Booking_Status = 'Cancelled';

update rides set final_paid_amount = round(0.5*Estimated_Fare,2)
where final_paid_amount IS null and booking_status = 'Incomplete';

update rides set final_paid_amount = Estimated_Fare
where final_paid_amount IS null and booking_status = 'Completed';

update ride_status set cancelled_by_driver = 0 
where cancelled_by_customer = 1 and cancelled_by_driver = 1;



-- =========================================
-- FEATURE ENGINEERING
-- =========================================
alter table rides
add column Ride_Date Date,
add column Ride_Month INT,
add column Ride_Day varchar(20),
add column Ride_Hour INT;

Update rides set 
Ride_Date = DATE(Booking_Timestamp),
Ride_Day = dayname(Booking_Timestamp),
Ride_Month = month(Booking_Timestamp),
Ride_Hour = hour(Booking_Timestamp);

Alter table rides 
add column Delay_Min INT;
update rides set
Delay_Min = Actual_Ride_Min - Estimated_Ride_min
where booking_Status='Completed';

Alter table rides
add column Ride_Length_Category VARCHAR(20);
Update rides SET Ride_Length_Category = 
CASE WHEN Ride_Distance_KM < 5 THEN 'Short'
WHEN Ride_Distance_KM BETWEEN 5 AND 15 THEN 'Medium' ELSE 'Long'
END;

Alter table rides
add column Operational_Loss DECIMAL(10,2);
UPDATE rides SET Operational_Loss =
CASE WHEN Booking_Status = 'Completed' THEN 
GREATEST(Estimated_Fare - Final_Paid_Amount,0)
ELSE 0  END;

Alter table rides
add column Cancellation_Loss DECIMAL(10,2);
UPDATE rides SET Cancellation_Loss =
CASE WHEN Booking_Status = 'Cancelled'
THEN Estimated_Fare ELSE 0 END;


-- =========================================
-- ENTITY INTEGRITY INFORCEMENT
-- =========================================

-- => Adding Primary Key constraint After cleaning
Alter table rides add primary key (Booking_ID);
Alter table customers add primary key (Customer_ID);
Alter table ride_status add primary key (Booking_ID);
Alter table drivers add primary key (Driver_ID);
Alter table vehicles add primary key (Vehicle_Type);


-- => Adding Foreign Key's
ALTER TABLE rides
ADD CONSTRAINT fk_customer
FOREIGN KEY (Customer_ID)
REFERENCES customers(Customer_ID);

ALTER TABLE rides
ADD CONSTRAINT fk_driver
FOREIGN KEY (Driver_ID)
REFERENCES drivers(Driver_ID);

ALTER TABLE rides
ADD CONSTRAINT fk_vehicle
FOREIGN KEY (Vehicle_Type)
REFERENCES vehicles(Vehicle_Type);



select * from vehicles;
select * from rides;
select * from ride_status;
select * from customers;
select * from drivers;

-- =========================================
-- ANALYTICAL QUERY SECTION
-- =========================================
# 1) Total Revenue 
select 
sum(final_paid_amount) as Total_Revenue 
from rides;

# 2) Total Rides 
select 
count(*) as Total_Rides 
from rides;

# 3) Completion Rate%
select 
round(sum(booking_status='Completed')*100/count(*),2)
as Completion_Rate_Pct
from rides;


# 4) Cancellation Rate%
select 
round(sum(booking_status='Cancelled')*100/count(*),2) 
as Cancellation_Rate_Pct
from rides;

# 5) Revenue Leakage% 
select 
sum(operational_loss+Cancellation_Loss)*100/sum(estimated_fare)
as Revenue_Leak_Pct
from rides;

# 6) Avg Ride value
select 
round(avg(Final_Paid_Amount),2) 
as Avg_Ride_Value
from rides;

# 7) Avg Ride Distance
select 
round(avg(ride_distance_km),2) 
as Avg_Ride_Distance_KM
from rides;

# 8) Repeat Customer%
with cust_cnt as 
(select customer_id,count(*) as RCount
from rides group by customer_id)
select Round(SUM(RCount>1)*100/count(*),2)
as Repeat_Cust_Pct
from cust_cnt;

# 9) Avg Delay Mins
select avg(delay_min) as Avg_Delay_Mins
from rides 
where booking_status='Completed';

-- =========================================
-- REVENUE ANALYSIS
-- =========================================

# 10) Monnthly Revenue Trend and MOM% Growth
with ct1 as 
(select date_format(booking_timestamp,'%Y-%m') Mnth
,sum(final_paid_amount) as Revenue
from rides
group by date_format(booking_timestamp,'%Y-%m'))
select Mnth,Revenue, 
round((Revenue - Lag(Revenue) over (order by mnth))
*100/Lag(Revenue) over (order by mnth),2) as MOM_Grwth_Pct
from ct1 order by mnth;


# 11) Total Rides and Revenue by day of week
select ride_day,count(*) as Total_Rides,
sum(final_paid_amount) as Revenue
from rides 
group by ride_day
order by ride_day;


# 12) Hour wise Ride Distribution
select ride_hour,
Count(*) as Ride_Count
from rides
group by ride_hour
order by ride_hour;

# 13) Average Delay by Hour
select ride_hour,
Round(avg(delay_min),2) as Avg_Delay
from rides
where booking_status='Completed'
group by ride_hour
order by ride_hour;

# 14) Cancellation rate by month
select date_format(booking_timestamp,'%Y-%m') as Mnth,
round(sum(booking_status='Cancelled')*100/count(*),2) as Cancellation_Rate
from rides
group by date_format(booking_timestamp,'%Y-%m')
order by mnth;


-- =========================================
-- CUSTOMER ANALYSIS
-- =========================================
# 15) Customer Revenue Ranking and Percentage Share
delimiter \\
create procedure Cust_rank(IN N int)
begin
	with cust_rev as
    (select customer_id, sum(final_paid_amount) as rev
    from rides group by customer_id),
    cte2 as
    (select *,sum(rev) over() as Total_Rev,
    Rank() over (order by rev desc) as Rev_Rank
	from cust_rev)
    select customer_id,rev,Rev_Rank, 
    round(rev*100/total_rev,2)
    from cte2 order by rev desc Limit N;
end \\
delimiter ;

call cust_rank(7);


# 16) Customer Ride Frequency Distribution
select dt.Ride_Count, count(*) Number_of_Cust from
(select customer_id, Count(*) as Ride_Count
from rides group by customer_id) dt group by Ride_Count order by Ride_Count;

# 17) Revenue by Customer City
select customer_city, sum(final_paid_amount) as Revenue
from rides r join customers c on r.customer_id=c.customer_id
group by customer_city order by revenue desc;

# 18) Customer Revenue Growth over time
with cust_month as 
(select customer_id,date_format(booking_timestamp,'%Y-%m') as Mnth 
,sum(final_paid_amount) as Revenue
from rides group by customer_id, date_format(booking_timestamp,'%Y-%m'))
select *, lag(revenue)  over (Partition by customer_id order by mnth) as Prev_Rev
from cust_month;


-- =========================================
-- DRIVER ANALYSIS
-- =========================================
# 19) Total rides by Driver
select driver_id,
count(*) as Total_Ride
from rides group by driver_id order by Total_Ride desc;

# 20) Top N Driver Revenue Performance
delimiter \\
create procedure driver_rank(IN N int)
begin  
	select driver_id, sum(final_paid_amount) as Driver_Rev,
    rank() over (order by sum(final_paid_amount) desc) as Drvr_Rnk
	from rides group by driver_id limit N;
end \\
delimiter ;

call driver_rank(7);

# 21) Driver Cancellation Rate
select r.driver_id,
round(sum(rs.cancelled_by_driver=1)*100/count(*),2) as Driver_Cancel_Rate
from rides r join ride_status rs on r.booking_id = rs.booking_id
group by driver_id order by Driver_Cancel_Rate Desc;

# 22) Driver Performance 
select driver_id, count(*) as Total_Rides,
round(sum(final_paid_amount),2) as Revenue,
avg(delay_min) as Avg_Delay
from rides where booking_status='Completed'
group by driver_id 
order by revenue desc;


-- =========================================
-- ADVANCED ANALYSIS
-- =========================================
# 23) Vehicle Economics
select v.Vehicle_Type,
count(r.Booking_ID) as Total_Rides,
SUM(r.Final_Paid_Amount) as Revenue,
avg(r.Ride_Distance_KM) as Avg_Distance
from rides r join vehicles v
on r.Vehicle_Type = v.Vehicle_Type
group by v.Vehicle_Type
order by Revenue desc;


# 24) Fare vs Actual Revenue
select v.Vehicle_Type, avg(r.Estimated_Fare) as Avg_Estimated_Fare,
avg(r.Final_Paid_Amount) as Avg_Final_Fare
from rides r join vehicles v on r.Vehicle_Type = v.Vehicle_Type
group by v.Vehicle_Type;


# 25) Cancellation Analysis
select Cancellation_Reason, COUNT(*) as Total_Cancellations
from ride_status
where Cancelled_By_Customer = 1 or Cancelled_By_Driver = 1
group by Cancellation_Reason order by Total_Cancellations desc;


# 26) Top Drivers Contributing to 80% of Platform Revenue
with driver_rev as 
(select Driver_ID,SUM(Final_Paid_Amount) AS Revenue
from rides where Booking_Status='Completed'
group by Driver_ID),
ranked as 
(select Driver_ID, Revenue,
sum(Revenue) over() as Total_Revenue,
sum(Revenue) over(order by Revenue desc) as Cum_Revenue
from driver_rev)
select Driver_ID,Revenue AS Driver_Revenue,
ROUND(Cum_Revenue*100/Total_Revenue,2) AS Cumulative_Revenue_Pct
from ranked where Cum_Revenue <= Total_Revenue*0.8;


-- =========================================
-- BUSINESS INSIGHTS
-- =========================================
# 27 Ride Length Category Analysis
select Ride_Length_Category, count(*) as Total_Rides,
sum(Final_Paid_Amount) as Revenue, avg(Ride_Distance_KM) as Avg_Distance
from rides group by Ride_Length_Category order by Revenue desc;

# 28 Peak Hour Revenue Analysis
select Peak_Hour_Flag, count(*) as Total_Rides,
sum(Final_Paid_Amount) as Revenue, avg(Final_Paid_Amount) as Avg_Ride_Value
from rides group by Peak_Hour_Flag;

# 29 Supply Demand Gap by Hour
select Ride_Hour, count(*) as Total_Requests,
Sum(Booking_Status='Completed') as Completed_Rides,
count(*) - SUM(Booking_Status='Completed') as Demand_Supply_Gap
from rides group by Ride_Hour order by Demand_Supply_Gap desc;




