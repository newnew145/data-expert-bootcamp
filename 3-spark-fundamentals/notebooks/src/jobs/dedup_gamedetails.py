from pyspark.sql import SparkSession

query = """
SELECT 
	game_id,
    team_id,
    team_abbreviation,
    team_city,
    player_id,
    player_name,
    nickname,
    start_position,
    comment,
    min,
    fgm,
    fga,
    fg_pct,
    fg3m,
    fg3a,
    fg3_pct,
    ftm,
    fta,
    ft_pct,
    oreb,
    dreb,
    reb,
    ast,
    stl,
    blk,
    turnovers,
    pf,
    pts,
    plus_minus
FROM (
	select *,
	row_number() over (partition by game_id,
									team_id,
									player_id
                        order by game_id,
									team_id,
									player_id
									) AS rn
	from game_details) dp
WHERE rn=1
"""


def do_dedup_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("game_details")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("games_detail") \
      .getOrCreate()
    output_df = do_dedup_transformation(spark, spark.table("game_details"))
    output_df.write.mode("overwrite").insertInto("dedup_games")

