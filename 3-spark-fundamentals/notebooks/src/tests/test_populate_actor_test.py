from chispa.dataframe_comparer import *
from ..jobs.populate_actor import do_populate_actors
from collections import namedtuple
from pyspark.sql import Row
ActorFilm = namedtuple("ActorFilm", "actor actorid film year votes rating filmid")
Actors = namedtuple("Actors", "actor actor_id films quality_class is_active year")



def test_populates_generation(spark):
    source_data = [
        ActorFilm('Brigitte Bardot', "nm0000003", "The Bear and the Doll", 1970, 431, 6.4, "tt0064779"),
        ActorFilm('Brigitte Bardot', "nm0000003", "Les novices", 1970, 219, 5.1, "tt0066164"),
        ActorFilm("Alan Bates", "nm0000869", "Three Sisters", 1970, 364, 6.5, "tt0066454")
    ]
    source_df = spark.createDataFrame(source_data)
    # destiation_df =  spark.createDataFrame([],destiation_data)

    actual_df = do_populate_actors(spark, source_df)
    expected_data = [
        Actors('Brigitte Bardot',
            "nm0000003",
            [
            Row(film="Les novices",votes=219,rating=5.1,filmid="tt0066164"),
            Row(film="The Bear and the Doll",votes=431,rating=6.4,filmid="tt0064779")
            ],
            "bad",
            bool(True),
            1970),
        Actors("Alan Bates",
               "nm0000869",
                [
                    Row(film="Three Sisters",votes=364,rating=6.5,filmid="tt0066454")
                ],
            "average",
            bool(True),
            1970
        )
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)