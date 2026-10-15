-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Build Drivers Standing View

-- COMMAND ----------

create or replace view formula1.gold.v_drivers_Standing
as
with driver_season_summary
as
(select r.season,
        d.driver_id,
        d.driver_name,
        d.nationality,
        count(*) as race_starts,
        sum(r.points) as total_points,
        count_if(r.is_win) as number_of_wins,
        count_if(r.is_podium) as number_of_podiums
from formula1.gold.fct_session_result r
join formula1.gold.dim_drivers d
on r.driver_id = d.driver_id
group by r.season,
        d.driver_id,
        d.driver_name,
        d.nationality)

select season,
        driver_id,
        driver_name,
        nationality,
        race_starts,
        rank() over(partition by season order by total_points desc, number_of_wins desc) as standings,
        total_points,
        number_of_wins,
        number_of_podiums
from driver_season_summary

-- COMMAND ----------

select * from formula1.gold.v_drivers_standing where season = 2025

-- COMMAND ----------

