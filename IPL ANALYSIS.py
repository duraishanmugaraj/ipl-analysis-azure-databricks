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

(
    team_df
    .write
    .format("delta")
    .mode("overwrite")
    .save("/mnt/ipl-data/processed/ipl_teams")
)

# COMMAND ----------

player = (
    player_df
    .withColumn('dob', F.date_format(F.to_date('dob', 'M/d/yyyy'), 'dd-MM-yyyy'))
    .withColumn('batting_hand', F.replace(F.col('batting_hand'), F.lit('�'), F.lit('')))
    .withColumn('bowling_skill', F.replace(F.col('bowling_skill'), F.lit('�'), F.lit('')))
)
(
    player
    .write
    .format("delta")
    .mode("overwrite")
    .save("/mnt/ipl-data/processed/ipl_players")
)

# COMMAND ----------

match = (
    match_df
    .drop("win_type")
    .withColumn('match_date', F.date_format(F.to_date('match_date', 'M/d/yyyy'), 'dd-MM-yyyy'))
)
(
    match
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("season_year")
    .save("/mnt/ipl-data/processed/ipl_match")
)

# COMMAND ----------

player_match = (
  player_match_df
  .drop("playermatch_key", "is_manofthematch", "isplayers_team_won", "batting_status", "bowling_status")
  .filter(F.col('player_match_sk') != F.lit(-1))
  .withColumn('batting_hand', F.replace(F.col('batting_hand'), F.lit('�'), F.lit('')))
  .withColumn('bowling_skill', F.replace(F.col('bowling_skill'), F.lit('�'), F.lit('')))
  .withColumn('dob', F.date_format(F.to_date('dob', 'M/d/yyyy'), 'dd-MM-yyyy'))
)
(
  player_match
  .write
  .format("delta")
  .mode("overwrite")
  .partitionBy("season_year")
  .save("/mnt/ipl-data/processed/ipl_player_match")
)

# COMMAND ----------

display(ball_by_ball_df.select("team_bowling").distinct())
display(ball_by_ball_df.select("team_batting").distinct())
display(team_df)
df = ball_by_ball_df.select("team_bowling").distinct()

display(ball_by_ball_df.join(team_df, df.team_bowling == team_df.team_name, "left").filter(F.col("team_id").isNotNull())) 

# COMMAND ----------

ball_by_ball = (
  ball_by_ball_df
  .drop("keeper_catch", "bowler_wicket", "obstructingfeild", "hit_wicket", "caught_and_bowled", "stumped","retired_hurt", "lbw", "run_out", "bowled", "caught")
  .withColumn('match_date', F.date_format(F.to_date('match_date', 'M/d/yyyy'), 'dd-MM-yyyy'))
)

(
  ball_by_ball_df
  .write
  .format("delta")
  .mode("overwrite")
  .partitionBy("season")
  .save("/mnt/ipl-data/processed/ipl_ball_by_ball")
)

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
