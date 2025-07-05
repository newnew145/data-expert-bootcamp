from chispa.dataframe_comparer import *
from ..jobs.dedup_gamedetails import do_dedup_transformation
from collections import namedtuple
from pyspark.sql import Row

Games = namedtuple("Games", "game_id \
    team_id \
    team_abbreviation \
    team_city \
    player_id \
    player_name \
    nickname \
    start_position \
    comment \
    min \
    fgm \
    fga \
    fg_pct \
    fg3m \
    fg3a \
    fg3_pct \
    ftm \
    fta \
    ft_pct \
    oreb \
    dreb \
    reb \
    ast \
    stl \
    blk \
    turnovers \
    pf \
    pts \
    plus_minus")

def test_dedup_gamedetails(spark):
    source_data = [
        Games(11600001,1610612744,"GSW","Golden State",256100,"David West",'','','',"12:36",2,5,0.4,1,2,0.5,1,2,0.5,1,5,6,0.0,1,1,3,3,6,9),
        Games(11600001,1610612744,"GSW","Golden State",256100,"David West",'','','',"12:36",2,5,0.4,1,2,0.5,1,2,0.5,1,5,6,0.0,1,1,3,3,6,9),
        Games(11600001,1610612744,"GSW","Golden State",256100,"David West",'','','',"12:36",2,5,0.4,1,2,0.5,1,2,0.5,1,5,6,0.0,1,1,3,3,6,9),
        Games(11600007,1610612756,"PHX","Phoenix",203933,"T.J. Warren",'',"F",'',"24:19",2,5,0.4,1,2,0.5,1,2,0.5,1,5,6,0.0,1,1,3,3,6,9),
        Games(11600007,1610612756,"PHX","Phoenix",203933,"T.J. Warren",'',"F",'',"24:19",2,5,0.4,1,2,0.5,1,2,0.5,1,5,6,0.0,1,1,3,3,6,9)
    ]

    source_df = spark.createDataFrame(source_data)

    actual_df = do_dedup_transformation(spark, source_df)

    expected_data = [
        Games(11600001,1610612744,"GSW","Golden State",256100,"David West",'','','',"12:36",2,5,0.4,1,2,0.5,1,2,0.5,1,5,6,0.0,1,1,3,3,6,9),
        Games(11600007,1610612756,"PHX","Phoenix",203933,"T.J. Warren",'',"F",'',"24:19",2,5,0.4,1,2,0.5,1,2,0.5,1,5,6,0.0,1,1,3,3,6,9)
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)