# Databricks notebook source
# dbutils.widgets.text("Year", "2008", "Year")
year = dbutils.widgets.get("Year")
print(f"Year widget value: {year}")


# COMMAND ----------

# MAGIC %run ./ipl_schemas

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName("IPL Process Load").getOrCreate()

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
    .filter(F.col('season_year') == year)
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
  .filter(F.col('season_year') == year)
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

ball_by_ball = (
  ball_by_ball_df
  .filter(F.col('season') == year)
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
