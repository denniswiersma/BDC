#!/usr/local/bin/python3.11

"""
Script for BDC assignment 5
"""

# IMPORTS

from Bio import SeqIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


# CLASSES
class MyClass:
    """
    Class to extract features from GenBank files and ansering questions
    """

    def __init__(self, filepath: str):
        self.filepath = filepath

    def extract_features(self):
        """
        Extract features from GenBank file using Biopython. Returns a list
        of dictionaries where each dict is a feature.
        """
        features = []

        with open(self.filepath) as handle:
            for record in SeqIO.parse(handle, "genbank"):
                for feature in record.features:
                    # < or > means feature is ambiguous so we drop
                    if "<" in str(feature.location) or ">" in str(
                        feature.location
                    ):
                        continue

                    feattype = feature.type
                    startloc = int(feature.location.start)
                    endloc = int(feature.location.end)
                    length = endloc - startloc

                    if feattype in ["CDS", "propeptide"]:
                        kind = "coding"
                    elif feattype in ["rRNA", "ncRNA"]:
                        kind = "noncoding"
                    elif feattype == "gene":
                        # skip cryptic genes if their location is contained in a CDS
                        if any(
                            feat.type == "CDS"
                            and feat.location.start >= feature.location.start
                            and feat.location.end <= feature.location.end
                            for feat in record.features
                        ):
                            continue
                        kind = "noncoding"
                    else:
                        continue

                    features.append(
                        {
                            "type": feattype,
                            "start": startloc,
                            "end": endloc,
                            "length": length,
                            "kind": kind,
                            "record_id": record.id,
                        }
                    )

        return features

    @staticmethod
    def question1(dataframe):
        """
        answer question 1
        """
        print("q1. Hoeveel 'features' heeft een Archaea genoom gemiddeld?")

        features_per_genome = dataframe.groupBy("record_id").agg(
            count("*").alias("feature_count")
        )
        mean_features = features_per_genome.agg(avg("feature_count")).first()[0]

        print(f"a1. Gemiddeld aantal features per genoom: {mean_features}")

    @staticmethod
    def question2(dataframe):
        """
        answer question 2
        """
        print(
            "q2. Hoe is de verhouding tussen coding en non-coding features? (Deel coding door non-coding totalen)."
        )

        coding_count = dataframe.filter(col("kind") == "coding").count()
        noncoding_count = dataframe.filter(col("kind") == "noncoding").count()
        ratio = coding_count / noncoding_count

        print(
            f"a2. Verhouding coding/non-coding: {ratio} ({coding_count}/{noncoding_count})"
        )

    @staticmethod
    def question3(dataframe):
        """
        answer question 3
        """
        print(
            "q3. Wat zijn de minimum en maximum aantal eiwitten van alle organismen in het file?"
        )

        cds_per_genome = (
            dataframe.filter(col("type") == "CDS")
            .groupBy("record_id")
            .agg(count("*").alias("cds_count"))
        )
        min_cds = cds_per_genome.agg({"cds_count": "min"}).first()[0]
        max_cds = cds_per_genome.agg({"cds_count": "max"}).first()[0]

        print(f"a3. Aantal eiwitten per genoom, min: {min_cds}, max: {max_cds}")

    @staticmethod
    def question4(dataframe):
        """
        answer question 4
        """
        print(
            "q4. Verwijder alle non-coding (RNA) features en schrijf dit weg als een apart DataFrame (Spark format)."
        )

        coding_df = dataframe.filter(col("kind") == "coding")
        coding_df.repartition(8).write.mode("overwrite").parquet(
            "coding_features.parquet"
        )
        print(
            "a4. Non-coding features verwijderd en weggeschreven naar coding_features.parquet"
        )

    @staticmethod
    def question5(dataframe):
        """
        answer question 5
        """
        print("q5. Wat is de gemiddelde lengte van een feature?")

        mean_len = dataframe.agg(avg("length")).first()[0]
        print(f"5. Gemiddelde lengte van een feature is {mean_len}")


# FUNCTIONS


# MAIN
def main():
    """
    Main function
    """
    filepath = "/data/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/refseq/release/archaea/archaea.1.genomic.gbff"

    myclass = MyClass(filepath)
    features = myclass.extract_features()

    spark = (
        SparkSession.builder.appName("BDC Assignment 5")
        .master("local[16]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType(
        [
            StructField("type", StringType(), True),
            StructField("start", IntegerType(), True),
            StructField("end", IntegerType(), True),
            StructField("length", IntegerType(), True),
            StructField("kind", StringType(), True),
            StructField("record_id", StringType(), True),
        ]
    )

    dataframe = spark.createDataFrame(features, schema=schema)

    myclass.question1(dataframe)
    myclass.question2(dataframe)
    myclass.question3(dataframe)
    myclass.question4(dataframe)
    myclass.question5(dataframe)


if __name__ == "__main__":
    main()
