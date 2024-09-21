# Databricks notebook source
# MAGIC %run ./ipl_schemas

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName("IPL Analysis").getOrCreate()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/ipl-data/raw/

# COMMAND ----------

team_df = spark.read.csv("/mnt/ipl-data/raw/Team.csv", header=True, schema=team_schema)
player_df = spark.read.csv("/mnt/ipl-data/raw/Player.csv", header=True, schema=player_schema)
match_df = spark.read.csv("/mnt/ipl-data/raw/Match.csv", header=True, schema=match_schema)
player_match_df = spark.read.csv("/mnt/ipl-data/raw/Player_match.csv", header=True, schema=player_match_schema)
ball_by_ball_df = spark.read.csv("/mnt/ipl-data/raw/Ball_By_Ball.csv", header=True, schema=ball_by_ball_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Team.csv

# COMMAND ----------

team_df.write.format("delta").mode("overwrite").save("/mnt/ipl-data/processed/ipl_teams")
player_df.write.format("delta").mode("overwrite").save("/mnt/ipl-data/delta/ipl_players")

# COMMAND ----------

match_df.write.format("delta").mode("overwrite").save("/mnt/ipl-data/delta/Match")
player_match_df.write.format("delta").mode("overwrite").save("/mnt/ipl-data/delta/Player_match")
ball_by_ball_df.write.format("delta").mode("overwrite").save("/mnt/ipl-data/delta/Ball_By_Ball")

# COMMAND ----------

display(team_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Player.csv
# MAGIC
# MAGIC - dob: convert the attribute to proper format 
# MAGIC - batting_hand: replace this -> � value to empty string
# MAGIC - bowling_skill: replace this lower case of 'N/A' with null 

# COMMAND ----------

display(player_df)

# COMMAND ----------

display(player_df.select("batting_hand").distinct())

# COMMAND ----------

display(player_df.select("bowling_skill").distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Match.csv
# MAGIC - match_date: convert the attribute to proper format
# MAGIC - win_type: drop this atribute

# COMMAND ----------

# match_df.select("win_type").distinct().show(100,False)
# +---------+
# |win_type |
# +---------+
# |wickets  |
# |NA       |
# |Tie      |
# |NULL     |
# |runs     |
# |NO Result|
# |run      |
# +---------+

display(match_df.filter(F.col('win_type').isNull()))

# COMMAND ----------

display(match_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Player_match.csv

# COMMAND ----------

# MAGIC %md
# MAGIC - drop these attributes: playermatch_key, is_manofthematch, isplayers_team_won, batting_status, bowling_status
# MAGIC - dob: batting_hand
# MAGIC - bowling_skill, bowling_skill: �
# MAGIC - remove -1 row

# COMMAND ----------

player_match_df.select("playermatch_key").distinct().show(100,False) 
player_match_df.select("is_manofthematch").distinct().show(100,False)  
player_match_df.select("isplayers_team_won").distinct().show(100,False)  
player_match_df.select("batting_status").distinct().show(100,False)  
player_match_df.select("bowling_status").distinct().show(100,False)  
player_match_df.select("bowling_skill").distinct().show(100,False)  
player_match_df.select("bowling_skill").distinct().show(100,False)  

# COMMAND ----------

display(player_match_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ball_By_Ball.csv

# COMMAND ----------

# drop keeper_catch bowler_wicket obstructingfeild hit_wicket caught_and_bowled stumped retired_hurt lbw run_out bowled caught 
# team_bowling,team_batting - replace team name with id
# match_date - date

# COMMAND ----------

display(ball_by_ball_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **UTIL FUNCTIONS**

# COMMAND ----------

# Utils 
def show_distinct_values(df):
    for col in df.columns: df.select(col).distinct().show(1000,False)

null_check = lambda df: df.select(
    *[
        F.sum(F.when((F.col(col).isNull()) | (F.lower(col).isin("null", "", "n/a")) , 1).otherwise(0)).alias(col + "_count")
        for col in df.columns
    ]
)

dup_check = lambda df: "No Duplicates Found" if df.count() == df.dropDuplicates().count() else "Duplicates Found"

dup_check_on_key = lambda df, key: "No Duplicates Found" if df.count() == df.dropDuplicates(key).count() else "Duplicates Found"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Player Data Frame Analysis

# COMMAND ----------

player_df.printSchema()

# COMMAND ----------

show_distinct_values(player_df)

# COMMAND ----------

display(null_check(player_df))

# COMMAND ----------

player_df.select("bowling_skill").filter(F.col("bowling_skill").startswith("�")).distinct().show(1000,False)

# COMMAND ----------

dup_check(player_df)

# COMMAND ----------

show_distinct_values(ball_by_ball_df)

# COMMAND ----------

ball_by_ball_df.groupBy("team_batting").count().show(100,False)

# COMMAND ----------

ball_by_ball_df.groupBy("team_bowling").count().show(100,False)

# COMMAND ----------

ball_by_ball_df.groupBy("season").count().show(100,False)

# COMMAND ----------

display(null_check(ball_by_ball_df))

# COMMAND ----------

display(dup_check(ball_by_ball_df))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Player Match Analysis

# COMMAND ----------

show_distinct_values(player_match_df)

# COMMAND ----------

display(null_check(player_match_df))

# COMMAND ----------

display(dup_check(player_match_df))
