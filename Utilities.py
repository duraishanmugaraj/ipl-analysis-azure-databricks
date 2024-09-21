# Databricks notebook source
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
