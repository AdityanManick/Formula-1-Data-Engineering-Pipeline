-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Building Constructors Standing View

-- COMMAND ----------

create or replace view formula1.gold.v_constructor_standing
as
with constructor_season_summary
as
(select 
  r.season,
  c.constructor_id,
  c.constructor_name,
  c.nationality,
  count(*) as race_starts,
  sum(r.points) as total_points,
  count_if(r.is_win) as number_of_wins,
  count_if(r.is_podium) as number_of_podiums
from formula1.gold.fct_session_result r
join formula1.gold.dim_constructors c
on r.constructor_id = c.constructor_id
group by   r.season,
  c.constructor_id,
  c.constructor_name,
  c.nationality)

select season,
  constructor_id,
  constructor_name,
  nationality,
  race_starts,
  rank() over(partition by season order by total_points desc, number_of_wins desc) as standing,
  total_points,
  number_of_wins,
  number_of_podiums
from constructor_season_summary

-- COMMAND ----------

select * from formula1.gold.v_constructor_standing where season = 2025    

-- COMMAND ----------

