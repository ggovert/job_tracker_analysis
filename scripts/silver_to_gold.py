import duckdb

def run_gold_logic(target_date=None):
    # 1. YOU MUST DEFINE 'con' FIRST
    con = duckdb.connect()
    
    # 2. Load the S3 extension
    con.execute("INSTALL httpfs; LOAD httpfs;")
    
    # 3. Configure the connection
    con.execute("""
        SET s3_endpoint='rustfs:9000';
        SET s3_access_key_id='rustfsadmin';
        SET s3_secret_access_key='rustfsadmin123';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)

# 4. Run the transformation with a Subquery to handle UNNEST
    try:
        con.execute(f"""
        COPY (
            WITH base_data AS (
                SELECT 
                    run_date,
                    id as job_id,
                    company,
                    location,
                    seniority,
                    text_clean as description,
                    unnest(roles) as role,
                    unnest(tech_stack) as tech
                FROM read_parquet('s3://silver/jobs_table/*/*.parquet', hive_partitioning=true)
            )
            SELECT * FROM base_data
        ) TO 's3://gold/full_job_market_analysis/' 
        (FORMAT PARQUET, PARTITION_BY run_date, OVERWRITE_OR_IGNORE true);
        """)
        
        print(f"✅ Gold Layer: full_job_market_analysis created successfully.")

    except Exception as e:
        print(f"⚠️ DuckDB Error: {e}")

if __name__ == "__main__":
    import sys
    # Use the date from the command line, or default to a test date
    date_arg = sys.argv[1] if len(sys.argv) > 1 else "2026-01-16"
    print(f"🚀 Starting manual Gold update for: {date_arg}")
    run_gold_logic(date_arg)