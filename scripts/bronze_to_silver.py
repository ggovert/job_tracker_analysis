from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import ArrayType, StringType
import pandas as pd
import sys
import re

# 1. Initialize Spark Session
# it will also inherit  s3 keys and master url from airflow operator
# spark = SparkSession.builder \
#     .getOrCreate()

spark = SparkSession.builder \
    .appName("IdempotentSilverLayer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://rustfs:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "rustfsadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "rustfsadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.change.detection.mode", "none") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

# spark = SparkSession.builder \
#     .appName("IdempotentSilverLayer") \
#     .config("spark.hadoop.fs.s3a.endpoint", "http://rustfs:9000") \
#     .config("spark.hadoop.fs.s3a.access.key", "rustfsadmin") \
#     .config("spark.hadoop.fs.s3a.secret.key", "rustfsadmin123") \
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
#     .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
#     .config("spark.hadoop.fs.s3a.committer.name", "directory") \
#     .getOrCreate()



# # 2. Handle data argument 
target_date = None
if len(sys.argv) > 1 and sys.argv[1] != "":
    target_date = sys.argv[1]
    input_path = f"s3a://bronze/hackernews/jobs_{target_date}*.json"
    print(f"Processing data for {target_date}")
else:
    input_path = "s3a://bronze/hackernews/*json"
    print("Processing all data")


#  3. Load the tech stack ref data (Metadata Bucket)

# path_to_tech_test = "file:///opt/airflow/scripts/standardized_tech_stack.json"
# path_to_jobs_test = "file:///opt/airflow/scripts/jobs_sample.json"


path_to_tech = "s3a://metadata/standardized_tech_stack.json"


df_tech_master = spark.read.option("multiline", "true").json(path_to_tech)
# tech_list_raw = [row.name_stack for row in df_tech_master.select("name_stack").collect()]
# tech_list = [tech.lower() for tech in tech_list_raw]

tech_map = {row.name_stack.lower(): row.standard_name for row in df_tech_master.collect()}

# Broadcast it so workers can access it efficiently
tech_map_broadcast = spark.sparkContext.broadcast(tech_map)


# 4. Load Raw Data (Bronze Bucket)
df_bronze = spark.read.option("multiline", "true").option("fileConstantMetadata", "true").json(input_path)


# 5. Cleaning logic: no id dedup and null text
df_cleaned = df_bronze \
    .filter(F.col("text").isNotNull()) \
    .withColumn("original_id", F.col("id")) \
    .dropDuplicates(["original_id"]) \
    .withColumn("comment_key", F.hash(F.col("text"))) \
    .dropDuplicates(["comment_key"]) \
    .withColumn("text_clean", F.lower(F.col("text")))

# 6. Extract company name from the text
# define extract_company_name function
# USING PANDAS UDF  
@pandas_udf(StringType())
def extract_company_name(series: pd.Series) -> pd.Series:
    """
    Extracts the company name from the text column.
    """
    def logic(text):
        # Null check
        if not isinstance(text, str):
            return "Unknown"

        # Standard Split Company Name : Company Name | Title | Area 
        if '|' in text:
            # split if found ()
            # return the first part and strip any leading/trailing whitespace
            company = text.split('|')[0].strip()
            if "(" in company:
                company = company.split("(")[0].strip()
            return company

        # Another alternative split  "-"
        if " - " in text or "—" in text:
            separator = "—" if "—" in text else " - "
            company = text.split(separator)[0].strip()
            
            if "(" in company:
                company = company.split("(")[0].strip()
            # This return handles both cases: with or without ( )
            return company
        
        if text.lower().startswith("at "):
            return text.lower().split("at ", 1)[1].strip()

        else:
            first_line_words = text.split("\n")[0].split()
            if first_line_words:
                potential_company = " ".join(first_line_words[:2])
                return f"[UNKNOWN]{potential_company}"

            # Fallback if no company name is found (just whitespace)
            return "Unknown"

    return series.apply(logic)

# Extract location from the text
@pandas_udf(StringType())
def extract_location_name(series: pd.Series) -> pd.Series:
    def logic(text):
        if not isinstance(text, str):
            return "Not Mentioned"
        
        # Normalize to lowercase once to save time
        content = text.lower()
        
        # 1. Check for Onsite (Highest Priority)
        # Includes variations like "In-Office" or "In person"
        if any(word in content for word in ["on-site", "onsite", "in-office", "in office", "in-person"]):
            return "Onsite"
        
        # 2. Check for Hybrid
        if "hybrid" in content:
            return "Hybrid"
        
        # 3. Check for Remote
        # Covers global, work from anywhere, and remote tags
        if any(word in content for word in ["remote", "anywhere", "wfh"]):
            return "Remote"
        
        # 4. Fallback
        return "Not Mentioned"

    return series.apply(logic)

@pandas_udf(ArrayType(StringType()))
def extract_roles_name(series: pd.Series) -> pd.Series:
    # 1. Basic safety check
    def logic(comment):
        if not comment:
            return []

        header = comment.split("\n")[0]

        # Keywords that indicate a job title
        role_keywords = ["engineer", "engineers", "swe", "manager", "lead", "developers","developer", "architect", "devrel", "senior", "staff", "product"]

        found_roles = []

        if "|" in header:
            parts = header.split("|")
            if len(parts) > 1:
                for part in parts[1:]:
                    part_lower = part.lower().strip()

                    # skip url
                    if part.strip().startswith("http"):
                        continue

                    # check for role keywords
                    if any(re.search(rf"\b{re.escape(keyword)}\b", part.lower()) for keyword in role_keywords):
                        # Clean the roel - remove everything after " -" if present
                        clean_role = part.split(" - ")[0].strip()
                        if clean_role:
                            found_roles.append(clean_role)
        return found_roles


    return series.apply(logic)

@pandas_udf(StringType())
def extract_seniority(series: pd.Series) -> pd.Series:
    def logic(roles_list):
        # If the roles list is empty or null, return None (Null in Spark)
        if not roles_list or len(roles_list) == 0:
            return None
        
        roles_text = " ".join(roles_list).lower()
        
        # Priority mapping
        if "principal" in roles_text:
            return "Principal"
        if "staff" in roles_text:
            return "Staff"
        if "lead" in roles_text:
            return "Lead"
        if "manager" in roles_text:
            return "Manager"
        if any(word in roles_text for word in ["senior", "sr"]):
            return "Senior"
        if any(word in roles_text for word in ["junior", "jr"]):
            return "Junior"
        if "intern" in roles_text:
            return "Intern"
        
        # If roles were found but no seniority keyword, then it's Mid
        return "Mid"

    return series.apply(logic)

@pandas_udf(ArrayType(StringType()))
def detect_tech_stack(series: pd.Series) -> pd.Series:
    local_map = tech_map_broadcast.value
    
    def logic(text):
        if not isinstance(text, str):
            return []
            
        text_lower = text.lower()
        found_standard_names = []
        
        for raw_key, standard_name in local_map.items():
            # Handle special characters like '/' in 'html/css' or '.' in 'node.js'
            pattern = rf"\b{re.escape(raw_key)}\b"
            
            if re.search(pattern, text_lower):
                found_standard_names.append(standard_name)
        
        return list(set(found_standard_names))

    return series.apply(logic)


# Apply the UDF to the text column
df_cleaned = df_cleaned.withColumn("company", extract_company_name(F.col("text"))) \
    .withColumn("location", extract_location_name(F.col("text"))) \
    .withColumn("roles", extract_roles_name(F.col("text"))) \
    .withColumn("seniority", extract_seniority(F.col("roles"))) \
    .withColumn("tech_stack", detect_tech_stack(F.col("text")))

if target_date:
    df_final = df_cleaned.withColumn("run_date", F.lit(target_date))
else:
    print(f" Run date is not specified, using file name: {F.input_file_name()}")
    df_final = df_cleaned.withColumn(
        "run_date", 
        F.regexp_extract(F.input_file_name(), r"jobs_(\d{4}-\d{2}-\d{2})", 1)
    )

df_final.select("run_date").distinct().show()

# The write command
df_final.write \
    .mode("overwrite") \
    .partitionBy("run_date") \
    .parquet("s3a://silver/jobs_table/")







# # -------------------------------
# # For testing purpose, output it to a file 
# # 1. Take a small sample (to make it fast and easy to read)
# df_sample = df_cleaned.select("company", "location", "roles","seniority","text")

# # 2. Coalesce(1) forces Spark to save it as ONE single file 
# # instead of 10 small parts
# output_path = "file:///opt/airflow/scripts/extraction_test"

# df_sample.coalesce(1).write \
#     .mode("overwrite") \
#     .option("header", "true") \
#     .parquet(output_path)

# print(f"✅ Sample saved to {output_path}")

# # -------------------------------

# # ------ For Checking Purpose ------
# # Show the first 50 results to verify the extraction
# df_cleaned.select("company", "location", "roles","seniority", "text").limit(50).show(truncate=50)

# # Optional: Count which companies appear the most to see if dedup worked
# df_cleaned.groupBy("company").count().orderBy(F.desc("count")).show()



'''
So okay help me step by step with my bronze to silver py



from pyspark.sql import SparkSession

from pyspark.sql import functions as F

from pyspark.sql.types import ArrayType, StringType

import sys





# Capture date argument from Airflow

if len(sys.argv) > 1:

    target_date = sys.argv[1]

else:

    # Fallback for manual testing

    target_date = "2026-01-15"



# 1. Initialize simple Spark Session

spark = SparkSession.builder.appName(f"Silver_Layer_{target_date}").getOrCreate()



# Define input path 

input_path = f"s3a://bronze/hackernews/jobs_{target_date}*.json"



#  2. Load the tech stack ref data (Metadata Bucket)

df_tech_master = spark.read.option("multiline", "true").json("s3a://metadata/standardized_tech_stack.json")

tech_list_raw = [row.name_stack for row in df_tech_master.select("name_stack").collect()]

tech_list = [tech.lower() for tech in tech_list_raw]





# 3. Load Raw Data (Bronze Bucket)

df_bronze = spark.read.option("multiline", "true").json(input_path)



# 4. Cleaning logic: no id dedup 

df_cleaned = df_bronze.dropDuplicates(["id"]) \

    .filter(F.col("text").isNotNull())



# 5. Extract company name from the text



# define extract_company_name function

def extract_company_name(text):

    """

    Extracts the company name from the text column.

    """

    if not isinstance(text, str):

        return None



    if '|' in text:

        # split if found ()

        # return the first part and strip any leading/trailing whitespace

        company = text.split('|')[0].strip()

        if "(" in company:

           company = company.split("(")[0].strip()



        return company

    if text.lower().startswith("at "):

        return text.lower().split("at ", 1)[1].strip()

    else:

        first_word = text.split()[0] if text.split() else "Unknown"

        return f"[UNKNOWN]{first_word}"



# Register the function as a UDF

extract_company_name_udf = F.udf(extract_company_name, StringType())



# Apply the UDF to the text column

df_cleaned = df_cleaned.withColumn("company", extract_company_name_udf(F.col("text")))







i just want to test it first to see if the extract of the company name works or not



can you help check on this first. 
'''