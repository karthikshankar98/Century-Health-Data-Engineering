"""
This is a boilerplate pipeline 'data_engineering_CH'
generated using Kedro 0.19.9
"""
import yaml
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, regexp_replace, expr, to_date, regexp_extract, sum
from pyspark.sql.functions import split, lower, explode, first, coalesce, trim, lit, DataFrame
from pyspark.sql.types import StringType
import subprocess
from functools import reduce

# Function to load column mappings as per Tuva data model
def load_column_mappings():
    with open(Path("conf/base/column_mappings.yml"), "r") as f:
        return yaml.safe_load(f)

column_mappings = load_column_mappings()

# Function for standardizing columns as per Tuva data model
def standardize_columns(data, dataset_name):

    mapping_key = f"{dataset_name}_mapping"
    mappings = column_mappings.get(mapping_key, {})

    return data.select([col(c).alias(mappings.get(c, c)) for c in data.columns])

# Function to clean and transform patients data
def clean_patients(patients, patient_gender):
    # Filter for Texas records only
    patients = patients.filter(col("STATE") == "Texas")

    # Merge patient data with gender information
    patients = patients.drop("GENDER")
    patients = patients.join(patient_gender, patients["PATIENT_ID"] == patient_gender["Id"], "left") \
                       .drop("Id")

    # Replace low and negative INCOME values with median
    income_stats = patients.select("INCOME").filter(col("INCOME") >= 35000) \
                            .approxQuantile("INCOME", [0.5], 0)
    median_income = income_stats[0]

    patients = patients.withColumn("INCOME", when(col("INCOME") < 35000, lit(median_income)) \
                       .otherwise(col("INCOME")))


    # Remove symbols/numbers from FIRST,LAST and MAIDEN names
    patients = patients.withColumn("FIRST", regexp_replace(col("FIRST"), r"[^a-zA-Z\s]", "")) \
                       .withColumn("LAST", regexp_replace(col("LAST"), r"[^a-zA-Z\s]", "")) \
                       .withColumn("MAIDEN", regexp_replace(col("LAST"), r"[^a-zA-Z\s]", ""))

    # Remove symbols/numbers from BIRTHPLACE field
    patients = patients.withColumn("BIRTHPLACE", regexp_replace(col("BIRTHPLACE"), r"[^a-zA-Z\s]", ""))

    # Fill missing DEATHDATE with placeholder
    patients = patients.withColumn("DEATHDATE", when(col("DEATHDATE").isNull(), "9999-12-31").otherwise(col("DEATHDATE")))
    patients = patients.drop(*["PREFIX","SUFFIX","MAIDEN","DRIVERS","PASSPORT","MARITAL","FIPS"])

    # Standardizing columns
    patients = standardize_columns(patients, "patients")
    patients_pd = patients.toPandas()

    return patients_pd


def clean_symptoms(symptoms):

    # Filter for extracting patients with only 'Lupus' symptoms
    symptoms = symptoms.withColumn("PATHOLOGY", lower("PATHOLOGY")) \
                       .filter(col("PATHOLOGY").like('%lupus%'))

    # Dropping 'GENDER', 'RACE', 'ETHNICITY' fields, already fetched from other tables
    symptoms = symptoms.drop(*["GENDER","RACE","ETHNICITY"])

    # Substituting NaN values of 'Age_End' with 9999
    symptoms = symptoms.withColumn("AGE_END", coalesce(col("AGE_END"), lit(9999)))


    # Splitting SYMPTOMS field values and creating 4 distinct SYMPTOM fields
    # The symptoms 'Rash', 'Fever', 'Fatigue' and 'Joint Pain' are now distinct fields with values
    symptom_data = symptoms.select("PATIENT", "SYMPTOMS")
    symptom_data = symptom_data.withColumn("symptom_kv", explode(split(col("SYMPTOMS"), ";")))

    symptom_data = symptom_data.withColumn("symptom", trim(expr("split(symptom_kv, ':')[0]"))) \
                               .withColumn("value", expr("split(symptom_kv, ':')[1]"))

    symptom_data = symptom_data.groupBy("PATIENT").pivot("symptom").agg(expr("first(value)"))
    symptoms = symptoms.drop("SYMPTOMS").join(symptom_data, on="PATIENT", how="left")

    # Recalculating 'NUM_SYMPTOMS' based on symptoms experienced by patients
    symptoms = symptoms.withColumn(
        "NUM_SYMPTOMS",
        reduce(
            lambda acc, c: acc + when(col(c) > 0, 1).otherwise(0),
            ["Rash", "Fever", "Fatigue", "Joint Pain"],
            lit(0)
        )
    )

    # Standardizing columns
    symptoms = standardize_columns(symptoms, "symptoms")
    symptoms_pd = symptoms.toPandas()

    return symptoms_pd

def clean_encounters(encounters):
    # Convert Patient ID to lowercase
    encounters = encounters.withColumn("PATIENT", lower(col("PATIENT")))

    # Filter for extracting only 'Lupus' encounters
    encounters = encounters.withColumn("REASONCODE", col("REASONCODE").cast("int"))
    encounters = encounters.filter(col("REASONCODE") == 200936003)

    # Standardize START and STOP dates
    encounters = encounters.withColumn("START", to_date(col("START"))) \
                           .withColumn("STOP", to_date(col("STOP")))

    # Standardizing columns
    encounters = standardize_columns(encounters, "encounters")
    encounters_pd = encounters.toPandas()

    return encounters_pd

def clean_conditions(conditions):

    # Filter for extracting only 'Lupus' conditions
    conditions = conditions.withColumn("CODE", col("CODE").cast("int"))
    conditions = conditions.filter(col("CODE") == 200936003)

    # Standardizing 'DESCRIPTION' field
    conditions = conditions.withColumn("DESCRIPTION", lower(col("DESCRIPTION")))

    # Fill missing dates STOP with placeholder '9999-12-31'
    conditions = conditions.withColumn("STOP", when(trim(col("STOP")) == "", None)
                           .otherwise(col("STOP"))
    )
    conditions = conditions.withColumn("STOP", coalesce(col("STOP"), lit("9999-12-31")))

    # Convert 'PATIENT ID' to lowercase to match keys in other tables
    conditions = conditions.withColumn("PATIENT", lower(col("PATIENT")))

    # Standardizing columns
    conditions = standardize_columns(conditions, "conditions")
    conditions_pd = conditions.toPandas()

    return conditions_pd

def clean_medications(medications):

    # Filter for extracting only 'Lupus' medications
    medications = medications.withColumn("REASONCODE", col("REASONCODE").cast("int"))
    medications = medications.filter(col("REASONCODE") == 200936003)


    # Fill missing dates STOP with placeholder '9999-12-31'
    medications = medications.withColumn("STOP", coalesce(col("STOP"), lit("9999-12-31")))
    medications = medications.withColumn("START", to_date(col("START")))
    medications = medications.withColumn("STOP", to_date(col("STOP")))

    # Convert ENCOUNTER ID to lowercase to match keys in other tables
    medications = medications.withColumn("ENCOUNTER", lower(col("ENCOUNTER")))

    # Standardizing 'DESCRIPTION' field
    medications = medications.withColumn("DESCRIPTION", lower(col("DESCRIPTION")))

    # Splitting 'DESCRIPTION' to extract useful data like medicine name, route and strength
    medications = medications.withColumn("MEDICATION_NAME", regexp_extract("DESCRIPTION", "([a-z]+)", 1)) \
        .withColumn("STRENGTH", regexp_extract("DESCRIPTION", r"(\d+\s?[a-z]*)", 1)) \
        .withColumn("ROUTE", regexp_extract("DESCRIPTION", r"([a-z]+)\s*(tablet|capsule|solution)", 1))

    # Standardizing columns
    medications = standardize_columns(medications, "medications")
    medications_pd = medications.toPandas()

    return medications_pd

def merge_data(conditions_table,encounters_table,patients_table,medications_table,symptoms_table):

    # merging all the datasets to create a master table
    merged_data = patients_table
    merged_data = merged_data.merge(encounters_table, on="patient_id", how="inner",suffixes=("", "_enc"))
    merged_data = merged_data.merge(medications_table, on="encounter_id", how="left",suffixes=("", "_med"))
    merged_data = merged_data.merge(conditions_table, on="encounter_id", how="left",suffixes=("", "_cond"))
    merged_data = merged_data.merge(symptoms_table, on="patient_id", how="left",suffixes=("", "_symp"))

    merged_data = merged_data.dropna(subset=['patient_id'])

    return merged_data