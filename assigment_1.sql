drop  schema assignment; 

create schema assignment;
use assignment;

select*
from bajaj;

select*
from eicher;

select*
from hero;

select*
from infosys;

select*
from tcs;

select*
from tvs;

-- loaded all the tables from the data file provided - note: date was imported as string due to issues with MySQL import tool

-- Creating tables for TCS first

-- Creating a temporary table to hold original data in the correct format ordered by Date
create table temp_tcs(
	select
		-- adding a row number to help with excluding invalid data
		row_number() over (order by str_to_date(tcs.date, '%d-%M-%Y')) as row_no,
		-- Changing the date to date time format from text format
		str_to_date(tcs.date, '%d-%M-%Y') as 'Date',
		`Close Price` as close_price
	from
		tcs
	order by 1
);

-- Creating the first table 'tcs1' with only valid 20 and 50 day moving averages
create table tcs1(
	select
		-- getting date and the close price
		`Date`, close_price,
        -- 20 day and 50 day moving average calculated using avg and over
		-- using an if function with row number from 'temp_tcs' to get only valid data ie moving averages after the 20th data point for '20 Day MA'
		IF( row_no > 20, avg(close_price) over (order by Date rows between 19 preceding and current row), NULL) as 20_Day_MA,
		-- using an if function with row number from 'temp_tcs' to get only valid data ie moving averages after the 50th data point for '50 Day MA'
    	IF( row_no > 50, avg(close_price) over (order by Date rows between 49 preceding and current row), NULL) as 50_Day_MA
	from
		temp_tcs
);

-- tcs1 table is ready, running select * to view contents
select * from tcs1;

-- drop the temporary formatting table as it is no longer needed
drop table temp_tcs;

-- creating the table 'tcs2' with trade signals based on moving averages from 'tcs1'
create table tcs2(
	SELECT
		-- getting date and the close price
		Date, close_price,
		-- Generating signal only when valid data is available, i.e. after the 50th data point by using IF
        -- IF the 50 day moving average is null, singal is also null as no valid signal is possible unless both 20 and 50 day moving averages are available
	    IF( 50_Day_MA IS NULL, NULL,
			-- otherwise (50_Day_MA is not null) use case-when to get the signal
			CASE
				-- flag is (MA_20_days - MA_50_days), generating previous flag using LAG function as LAG(MA_20_days - MA_50_days)
				-- if flag > 0 and flagp < 0, signal is "BUY"
				WHEN (20_Day_MA - 50_Day_MA) > 0 AND (LAG(20_Day_MA - 50_Day_MA) OVER (ORDER BY `Date`)) < 0 THEN "BUY"
				-- if flag < 0 and flagp > 0, signal is "SELL"
				WHEN (20_Day_MA - 50_Day_MA) < 0 AND (LAG(20_Day_MA - 50_Day_MA) OVER (ORDER BY `Date`)) > 0 THEN "SELL"
				-- in other cases, where both flag and flagp are < 0 or > 0 , singal is "HOLD"
				ELSE "HOLD"
			END) as s_signal
	FROM
		tcs1
);

-- tcs2 table is ready, running select * to view contents
select * from tcs2;

/***********************************************************/

-- Creating tables for BAJAJ

-- Creating a temporary table to hold original data in the correct format ordered by Date
create table temp_bajaj(
	select
		-- adding a row number to help with excluding invalid data
		row_number() over (order by str_to_date(bajaj.date, '%d-%M-%Y')) as row_no,
		-- Changing the date to date time format from text format
		str_to_date(bajaj.date, '%d-%M-%Y') as 'Date',
		`Close Price` as close_price
	from
		bajaj
	order by 1
);

-- Creating the first table 'bajaj1' with only valid 20 and 50 day moving averages
create table bajaj1(
	select
		-- getting date and the close price
		`Date`, close_price,
        -- 20 day and 50 day moving average calculated using avg and over
		-- using an if function with row number from 'temp_bajaj' to get only valid data ie moving averages after the 20th data point for '20 Day MA'
		IF( row_no > 20, avg(close_price) over (order by Date rows between 19 preceding and current row), NULL) as 20_Day_MA,
		-- using an if function with row number from 'temp_bajaj' to get only valid data ie moving averages after the 50th data point for '50 Day MA'
    	IF( row_no > 50, avg(close_price) over (order by Date rows between 49 preceding and current row), NULL) as 50_Day_MA
	from
		temp_bajaj
);

-- bajaj1 table is ready, running select * to view contents
select * from bajaj1;

-- drop the temporary formatting table as it is no longer needed
drop table temp_bajaj;

-- creating the table 'bajaj2' with trade signals based on moving averages from 'bajaj1'
create table bajaj2(
	SELECT
		-- getting date and the close price
		Date, close_price,
		-- Generating signal only when valid data is available, i.e. after the 50th data point by using IF
        -- IF the 50 day moving average is null, singal is also null as no valid signal is possible unless both 20 and 50 day moving averages are available
	    IF( 50_Day_MA IS NULL, NULL,
			-- otherwise (50_Day_MA is not null) use case-when to get the signal
			CASE
				-- flag is (MA_20_days - MA_50_days), generating previous flag using LAG function as LAG(MA_20_days - MA_50_days)
				-- if flag > 0 and flagp < 0, signal is "BUY"
				WHEN (20_Day_MA - 50_Day_MA) > 0 AND (LAG(20_Day_MA - 50_Day_MA) OVER (ORDER BY `Date`)) < 0 THEN "BUY"
				-- if flag < 0 and flagp > 0, signal is "SELL"
				WHEN (20_Day_MA - 50_Day_MA) < 0 AND (LAG(20_Day_MA - 50_Day_MA) OVER (ORDER BY `Date`)) > 0 THEN "SELL"
				-- in other cases, where both flag and flagp are < 0 or > 0 , singal is "HOLD"
				ELSE "HOLD"
			END) as s_signal
	FROM
		bajaj1
);

-- bajaj2 table is ready, running select * to view contents
select * from bajaj2;

/***********************************************************/

-- Creating tables for TVS

-- Creating a temporary table to hold original data in the correct format ordered by Date
create table temp_tvs(
	select
		-- adding a row number to help with excluding invalid data
		row_number() over (order by str_to_date(tvs.date, '%d-%M-%Y')) as row_no,
		-- Changing the date to date time format from text format
		str_to_date(tvs.date, '%d-%M-%Y') as 'Date',
		`Close Price` as close_price
	from
		tvs
	order by 1
);

-- Creating the first table 'tvs1' with only valid 20 and 50 day moving averages
create table tvs1(
	select
		-- getting date and the close price
		`Date`, close_price,
        -- 20 day and 50 day moving average calculated using avg and over
		-- using an if function with row number from 'temp_tvs' to get only valid data ie moving averages after the 20th data point for '20 Day MA'
		IF( row_no > 20, avg(close_price) over (order by Date rows between 19 preceding and current row), NULL) as 20_Day_MA,
		-- using an if function with row number from 'temp_tvs' to get only valid data ie moving averages after the 50th data point for '50 Day MA'
    	IF( row_no > 50, avg(close_price) over (order by Date rows between 49 preceding and current row), NULL) as 50_Day_MA
	from
		temp_tvs
);

-- tvs1 table is ready, running select * to view contents
select * from tvs1;

-- drop the temporary formatting table as it is no longer needed
drop table temp_tvs;

-- creating the table 'tvs2' with trade signals based on moving averages from 'tvs1'
create table tvs2(
	SELECT
		-- getting date and the close price
		Date, close_price,
		-- Generating signal only when valid data is available, i.e. after the 50th data point by using IF
        -- IF the 50 day moving average is null, singal is also null as no valid signal is possible unless both 20 and 50 day moving averages are available
	    IF( 50_Day_MA IS NULL, NULL,
			-- otherwise (50_Day_MA is not null) use case-when to get the signal
			CASE
				-- flag is (MA_20_days - MA_50_days), generating previous flag using LAG function as LAG(MA_20_days - MA_50_days)
				-- if flag > 0 and flagp < 0, signal is "BUY"
				WHEN (20_Day_MA - 50_Day_MA) > 0 AND (LAG(20_Day_MA - 50_Day_MA) OVER (ORDER BY `Date`)) < 0 THEN "BUY"
				-- if flag < 0 and flagp > 0, signal is "SELL"
				WHEN (20_Day_MA - 50_Day_MA) < 0 AND (LAG(20_Day_MA - 50_Day_MA) OVER (ORDER BY `Date`)) > 0 THEN "SELL"
				-- in other cases, where both flag and flagp are < 0 or > 0 , singal is "HOLD"
				ELSE "HOLD"
			END) as s_signal
	FROM
		tvs1
);

-- tvs2 table is ready, running select * to view contents
select * from tvs2;

/***********************************************************/

-- Creating tables for INFOSYS

-- Creating a temporary table to hold original data in the correct format ordered by Date
create table temp_infosys(
	select
		-- adding a row number to help with excluding invalid data
		row_number() over (order by str_to_date(infosys.date, '%d-%M-%Y')) as row_no,
		-- Changing the date to date time format from text format
		str_to_date(infosys.date, '%d-%M-%Y') as 'Date',
		`Close Price` as close_price
	from
		infosys
	order by 1
);

-- Creating the first table 'infosys1' with only valid 20 and 50 day moving averages
create table infosys1(
	select
		-- getting date and the close price
		`Date`, close_price,
        -- 20 day and 50 day moving average calculated using avg and over
		-- using an if function with row number from 'temp_infosys' to get only valid data ie moving averages after the 20th data point for '20 Day MA'
		IF( row_no > 20, avg(close_price) over (order by Date rows between 19 preceding and current row), NULL) as 20_Day_MA,
		-- using an if function with row number from 'temp_infosys' to get only valid data ie moving averages after the 50th data point for '50 Day MA'
    	IF( row_no > 50, avg(close_price) over (order by Date rows between 49 preceding and current row), NULL) as 50_Day_MA
	from
		temp_infosys
);

-- infosys1 table is ready, running select * to view contents
select * from infosys1;

-- drop the temporary formatting table as it is no longer needed
drop table temp_infosys;

-- creating the table 'infosys2' with trade signals based on moving averages from 'infosys1'
create table infosys2(
	SELECT
		-- getting date and the close price
		Date, close_price,
		-- Generating signal only when valid data is available, i.e. after the 50th data point by using IF
        -- IF the 50 day moving average is null, singal is also null as no valid signal is possible unless both 20 and 50 day moving averages are available
	    IF( 50_Day_MA IS NULL, NULL,
			-- otherwise (50_Day_MA is not null) use case-when to get the signal
			CASE
				-- flag is (MA_20_days - MA_50_days), generating previous flag using LAG function as LAG(MA_20_days - MA_50_days)
				-- if flag > 0 and flagp < 0, signal is "BUY"
				WHEN (20_Day_MA - 50_Day_MA) > 0 AND (LAG(20_Day_MA - 50_Day_MA) OVER (ORDER BY `Date`)) < 0 THEN "BUY"
				-- if flag < 0 and flagp > 0, signal is "SELL"
				WHEN (20_Day_MA - 50_Day_MA) < 0 AND (LAG(20_Day_MA - 50_Day_MA) OVER (ORDER BY `Date`)) > 0 THEN "SELL"
				-- in other cases, where both flag and flagp are < 0 or > 0 , singal is "HOLD"
				ELSE "HOLD"
			END) as s_signal
	FROM
		infosys1
);

-- infosys2 table is ready, running select * to view contents
select * from infosys2;

/***********************************************************/

-- Creating tables for EICHER

-- Creating a temporary table to hold original data in the correct format ordered by Date
create table temp_eicher(
	select
		-- adding a row number to help with excluding invalid data
		row_number() over (order by str_to_date(eicher.date, '%d-%M-%Y')) as row_no,
		-- Changing the date to date time format from text format
		str_to_date(eicher.date, '%d-%M-%Y') as 'Date',
		`Close Price` as close_price
	from
		eicher
	order by 1
);

-- Creating the first table 'eicher1' with only valid 20 and 50 day moving averages
create table eicher1(
	select
		-- getting date and the close price
		`Date`, close_price,
        -- 20 day and 50 day moving average calculated using avg and over
		-- using an if function with row number from 'temp_eicher' to get only valid data ie moving averages after the 20th data point for '20 Day MA'
		IF( row_no > 20, avg(close_price) over (order by Date rows between 19 preceding and current row), NULL) as 20_Day_MA,
		-- using an if function with row number from 'temp_eicher' to get only valid data ie moving averages after the 50th data point for '50 Day MA'
    	IF( row_no > 50, avg(close_price) over (order by Date rows between 49 preceding and current row), NULL) as 50_Day_MA
	from
		temp_eicher
);

-- eicher1 table is ready, running select * to view contents
select * from eicher1;

-- drop the temporary formatting table as it is no longer needed
drop table temp_eicher;

-- creating the table 'eicher2' with trade signals based on moving averages from 'eicher1'
create table eicher2(
	SELECT
		-- getting date and the close price
		Date, close_price,
		-- Generating signal only when valid data is available, i.e. after the 50th data point by using IF
        -- IF the 50 day moving average is null, singal is also null as no valid signal is possible unless both 20 and 50 day moving averages are available
	    IF( 50_Day_MA IS NULL, NULL,
			-- otherwise (50_Day_MA is not null) use case-when to get the signal
			CASE
				-- flag is (MA_20_days - MA_50_days), generating previous flag using LAG function as LAG(MA_20_days - MA_50_days)
				-- if flag > 0 and flagp < 0, signal is "BUY"
				WHEN (20_Day_MA - 50_Day_MA) > 0 AND (LAG(20_Day_MA - 50_Day_MA) OVER (ORDER BY `Date`)) < 0 THEN "BUY"
				-- if flag < 0 and flagp > 0, signal is "SELL"
				WHEN (20_Day_MA - 50_Day_MA) < 0 AND (LAG(20_Day_MA - 50_Day_MA) OVER (ORDER BY `Date`)) > 0 THEN "SELL"
				-- in other cases, where both flag and flagp are < 0 or > 0 , singal is "HOLD"
				ELSE "HOLD"
			END) as s_signal
	FROM
		eicher1
);

-- eicher2 table is ready, running select * to view contents
select * from eicher2;

/***********************************************************/

-- Creating tables for HERO

-- Creating a temporary table to hold original data in the correct format ordered by Date
create table temp_hero(
	select
		-- adding a row number to help with excluding invalid data
		row_number() over (order by str_to_date(hero.date, '%d-%M-%Y')) as row_no,
		-- Changing the date to date time format from text format
		str_to_date(hero.date, '%d-%M-%Y') as 'Date',
		`Close Price` as close_price
	from
		hero
	order by 1
);

-- Creating the first table 'hero1' with only valid 20 and 50 day moving averages
create table hero1(
	select
		-- getting date and the close price
		`Date`, close_price,
        -- 20 day and 50 day moving average calculated using avg and over
		-- using an if function with row number from 'temp_hero' to get only valid data ie moving averages after the 20th data point for '20 Day MA'
		IF( row_no > 20, avg(close_price) over (order by Date rows between 19 preceding and current row), NULL) as 20_Day_MA,
		-- using an if function with row number from 'temp_hero' to get only valid data ie moving averages after the 50th data point for '50 Day MA'
    	IF( row_no > 50, avg(close_price) over (order by Date rows between 49 preceding and current row), NULL) as 50_Day_MA
	from
		temp_hero
);

-- hero1 table is ready, running select * to view contents
select * from hero1;

-- drop the temporary formatting table as it is no longer needed
drop table temp_hero;

-- creating the table 'hero2' with trade signals based on moving averages from 'hero1'
create table hero2(
	SELECT
		-- getting date and the close price
		Date, close_price,
		-- Generating signal only when valid data is available, i.e. after the 50th data point by using IF
        -- IF the 50 day moving average is null, singal is also null as no valid signal is possible unless both 20 and 50 day moving averages are available
	    IF( 50_Day_MA IS NULL, NULL,
			-- otherwise (50_Day_MA is not null) use case-when to get the signal
			CASE
				-- flag is (MA_20_days - MA_50_days), generating previous flag using LAG function as LAG(MA_20_days - MA_50_days)
				-- if flag > 0 and flagp < 0, signal is "BUY"
				WHEN (20_Day_MA - 50_Day_MA) > 0 AND (LAG(20_Day_MA - 50_Day_MA) OVER (ORDER BY `Date`)) < 0 THEN "BUY"
				-- if flag < 0 and flagp > 0, signal is "SELL"
				WHEN (20_Day_MA - 50_Day_MA) < 0 AND (LAG(20_Day_MA - 50_Day_MA) OVER (ORDER BY `Date`)) > 0 THEN "SELL"
				-- in other cases, where both flag and flagp are < 0 or > 0 , singal is "HOLD"
				ELSE "HOLD"
			END) as s_signal
	FROM
		hero1
);

-- hero2 table is ready, running select * to view contents
select * from hero2;

describe hero1;

create table master_table(d_date date, bajaj double, TCS double, TVS double, Infosys double, Eicher double, Hero double);

insert into master_table(d_date, bajaj) select `date`,`close_price` from bajaj1;
update master_table set TCS = (select close_price from tcs1 where master_table.d_date = tcs1.date);
update master_table set TVS = (select close_price from tvs1 where master_table.d_date = tvs1.date);
update master_table set Infosys = (select close_price from infosys1 where master_table.d_date = infosys1.date);
update master_table set Eicher = (select close_price from eicher1 where master_table.d_date = eicher1.date);
update master_table set Hero = (select close_price from hero1 where master_table.d_date = hero1.date);

select * from master_table;

select * from master_table;

-- created a user defined with s_signal where you give the input to the d for the date 
-- decalre a varchar as d_value which selects the singnal value for that particular date 
 
delimiter $$
create function  s_signal(d date)
returns varchar(10) deterministic
begin
	declare d_value varchar(5);
	set d_value = (select s_signal from bajaj2 where Date = d);
	return d_value;
end
$$
delimiter ;
select s_signal('2018-05-29') as s_signal;



