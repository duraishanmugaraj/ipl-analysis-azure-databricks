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

spark = SparkSession.builder.appName("IPL Presentation Load").getOrCreate()

# COMMAND ----------

team_df = (
    spark
    .read
    .format("delta")
    .load("/mnt/ipl-data/processed/ipl_teams")
)
player_df = (
    spark
    .read
    .format("delta")
    .load("/mnt/ipl-data/processed/ipl_players")
)

# COMMAND ----------

match_df = (
    spark
    .read
    .format("delta")
    .load("/mnt/ipl-data/processed/ipl_match")
    .filter(F.col('season_year') == year)
)

player_match_df = (
    spark
    .read
    .format("delta")
    .load("/mnt/ipl-data/processed/ipl_player_match")
    .filter(F.col('season_year') == year)
)

ball_by_ball_df = (
    spark
    .read
    .format("delta")
    .load("/mnt/ipl-data/processed/ipl_ball_by_ball")
    .filter(F.col('season') == year)
)

# COMMAND ----------

dbutils.fs.ls("mnt/ipl-data/presentation")

# COMMAND ----------

# DBTITLE 1,1. Top Scoring Batsmen per Season
top_scoring_batsmen_per_season = (
    ball_by_ball_df.join(match_df, ball_by_ball_df.match_id == match_df.match_id)
    .join(
        player_match_df,
        (match_df.match_id == player_match_df.match_id)
        & (ball_by_ball_df.striker == player_match_df.player_id),
    )
    .join(player_df, player_df.player_id == player_match_df.player_id)
    .groupBy(player_df.player_name, match_df.season_year)
    .agg(F.sum(ball_by_ball_df.runs_scored).alias("total_runs"))
    .orderBy(match_df.season_year, F.desc("total_runs"))
)

(
    top_scoring_batsmen_per_season
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("season_year")
    .save("/mnt/ipl-data/presentation/report1")
)

# COMMAND ----------

# DBTITLE 1,2. Economical Bowlers in Powerplay
economical_bowlers_powerplay = (
    ball_by_ball_df.join(
        player_match_df,
        (ball_by_ball_df.match_id == player_match_df.match_id)
        & (ball_by_ball_df.bowler == player_match_df.player_id),
    )
    .join(player_df, player_match_df.player_id == player_df.player_id)
    .where(ball_by_ball_df.over_id <= 6)
    .groupBy(player_df.player_name)
    .agg(
        F.avg(ball_by_ball_df.runs_scored).alias("avg_runs_per_ball"),
        F.count(ball_by_ball_df.bowler_wicket).alias("total_wickets"),
    )
    .filter(F.count("*") >= 1)
    .withColumn("season_year", F.lit(year))
    .orderBy("avg_runs_per_ball", F.desc("total_wickets"))
)

(
    economical_bowlers_powerplay
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("season_year")
    .save("/mnt/ipl-data/presentation/report2")
)

# COMMAND ----------

# DBTITLE 1,3. Toss Impact on Individual Matches
toss_impact_individual_matches = (
    match_df.filter(match_df.toss_name.isNotNull())
    .select(
        match_df.match_id,
        match_df.toss_winner,
        match_df.toss_name,
        match_df.match_winner,
        F.when(match_df.toss_winner == match_df.match_winner, "Won")
        .otherwise("Lost")
        .alias("match_outcome"),
    )
    .withColumn("season_year", F.lit(year))
    .orderBy("match_id")
)

(
    toss_impact_individual_matches
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("season_year")
    .save("/mnt/ipl-data/presentation/report3")
)

# COMMAND ----------

# DBTITLE 1,4. Average Runs in Wins
average_runs_in_wins = (
    ball_by_ball_df.join(
        player_match_df,
        (ball_by_ball_df.match_id == player_match_df.match_id)
        & (ball_by_ball_df.striker == player_match_df.player_id),
    )
    .join(player_df, player_match_df.player_id == player_df.player_id)
    .join(match_df, player_match_df.match_id == match_df.match_id)
    .filter(match_df.match_winner == player_match_df.player_team)
    .groupBy(player_df.player_name)
    .agg(
        F.avg(ball_by_ball_df.runs_scored).alias("avg_runs_in_wins"),
        F.count("*").alias("innings_played"),
    )
    .withColumn("season_year", F.lit(year))
    .orderBy("avg_runs_in_wins", ascending=True)
)

(
    average_runs_in_wins
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("season_year")
    .save("/mnt/ipl-data/presentation/report4")
)

# COMMAND ----------

# DBTITLE 1,5. Scores by Venue
scores_by_venue = (
    ball_by_ball_df.join(match_df, ball_by_ball_df.match_id == match_df.match_id)
    .groupBy(ball_by_ball_df.match_id, match_df.venue_name)
    .agg(F.sum(ball_by_ball_df.runs_scored).alias("total_runs"))
    .groupBy("venue_name")
    .agg(
        F.avg("total_runs").alias("average_score"),
        F.max("total_runs").alias("highest_score"),
    )
    .withColumn("season_year", F.lit(year))
    .orderBy("average_score", ascending=False)
)

(
    scores_by_venue
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("season_year")
    .save("/mnt/ipl-data/presentation/report5")
)
