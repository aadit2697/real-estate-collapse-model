import os
import requests
import pandas as pd
import numpy as np
from source.utils.snowflake_connector import SnowflakeConnector
from source.utils.snowflake_schema_manager import SnowflakeSchemaManager

class HousingDataProcessor:
    def __init__(self):
        """Initialize the data processor with required configurations."""
        self.sf_connector = SnowflakeConnector()
        self.schema_manager = SnowflakeSchemaManager()
        self.raw_data_path = 'data/processed/'

        # ✅ Ensure data directory exists
        os.makedirs(self.raw_data_path, exist_ok=True)

    # 🔹 Step 1: Download Data from GitHub
    def download_data_from_github(self):
        """Download raw CSV and Excel files from GitHub and save locally."""
        csv_files = {
            "one_family_units_quarterly.csv": "https://raw.githubusercontent.com/aadit2697/real-estate-collapse-model/main/data/processed/one_family_units_quarterly.csv",
            "multi_units_buildings_quarterly.csv": "https://raw.githubusercontent.com/aadit2697/real-estate-collapse-model/main/data/processed/multi_units_buildings_quarterly.csv",
            "home_price_index_quarterly.csv":"https://raw.githubusercontent.com/aadit2697/real-estate-collapse-model/main/data/processed/home_price_index_quarterly.csv",
            "mortgage_rate_quarterly.csv":"https://raw.githubusercontent.com/aadit2697/real-estate-collapse-model/main/data/processed/mortgage_rate_quarterly.csv",
            "unemployment_rate_quarterly.csv":"https://raw.githubusercontent.com/aadit2697/real-estate-collapse-model/main/data/processed/unemployment_rate_quarterly.csv",
            "cpi_quarterly.csv":"https://raw.githubusercontent.com/aadit2697/real-estate-collapse-model/main/data/processed/cpi_quarterly.csv",
        }

        # excel_files = {
        #     "quar_starts_purpose_cust.xlsx": "https://raw.githubusercontent.com/aadit2697/real-estate-collapse-model/kapil_dev/data/raw/quar_starts_purpose_cust.xlsx",
        #     "starts_cust.xlsx": "https://raw.githubusercontent.com/aadit2697/real-estate-collapse-model/kapil_dev/data/raw/starts_cust.xlsx",
        #     "startsunpub_starts_cust.xlsx": "https://raw.githubusercontent.com/aadit2697/real-estate-collapse-model/kapil_dev/data/raw/startsunpub_starts_cust.xlsx"
        # }

        # ✅ Download CSV files
        for filename, url in csv_files.items():
            response = requests.get(url)
            if response.status_code == 200:
                with open(f"{self.raw_data_path}{filename}", "wb") as file:
                    file.write(response.content)
                print(f"✅ {filename} downloaded successfully from GitHub.")
            else:
                print(f"❌ Failed to download {filename}.")

        # ✅ Download and convert Excel files to CSV
        # for filename, url in excel_files.items():
        #     response = requests.get(url)
        #     if response.status_code == 200:
        #         file_path = f"{self.raw_data_path}{filename}"
        #         with open(file_path, "wb") as file:
        #             file.write(response.content)
        #         print(f"✅ {filename} downloaded successfully.")

        #         # Convert Excel to CSV
        #         csv_path = file_path.replace(".xlsx", ".csv")
        #         df = pd.read_excel(file_path, skiprows=5)
        #         df.to_csv(csv_path, index=False)
        #         print(f"✅ Converted {filename} to CSV format.")
        #     else:
        #         print(f"❌ Failed to download {filename}.")

    # 🔹 Step 2: Upload Data to Snowflake Stage
    def upload_data_to_snowflake_stage(self, conn):
        """Upload transformed quarterly CSV files to the correct Snowflake stage."""
        print("🚀 Starting upload to Snowflake Staging...")
        cur = conn.cursor()

        processed_data_path = "data/processed/"

        for filename in os.listdir(processed_data_path):
            if filename.endswith(".csv"):
                file_path = os.path.abspath(f"{processed_data_path}{filename}")

                # ✅ Determine which stage to use
                if "home_price_index" in filename.lower():
                    stage = "housing_data_stage"
                elif "mortgage_rate" in filename.lower():
                    stage = "mortgage_data_stage"
                elif "one_family" in filename.lower() or "multi_units" in filename.lower():
                    stage = "starts_data_stage"
                elif "unemployment" in filename.lower() or "cpi" in filename.lower():
                    stage = "cpi_unemp_data_stage"
                else:
                    print(f"⚠️ Skipping unrecognized file: {filename}")
                    continue

                print(f"🔹 Uploading {filename} to {stage} ...")
                cur.execute(f"PUT file://{file_path} @{stage} AUTO_COMPRESS=FALSE;")
                print(f"✅ Uploaded {filename} to {stage}")

        conn.commit()
        cur.close()
        print("✅ All files uploaded to Snowflake Staging.")

    # 🔹 Step 3: Load Staged Data into Snowflake Tables
    def load_staged_data_to_snowflake_tables(self, conn):
        """Load data from Snowflake staging area into appropriately structured staging tables."""
        cur = conn.cursor()

        print("🚀 Loading staged data into Snowflake tables...")

        # ✅ Load Home Price Index data
        cur.execute("""
            CREATE OR REPLACE TABLE housing_price_staging (
                Period STRING,
                Quarterly_avg_Home_Price_Index FLOAT
            );
        """)
        cur.execute("""
            COPY INTO housing_price_staging
            FROM @housing_data_stage/home_price_index_quarterly.csv
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
        """)

        # ✅ Load Mortgage Rate data
        cur.execute("""
            CREATE OR REPLACE TABLE mortgage_rates_staging (
                Period STRING,
                Quarterly_avg_Mortgage_Rate FLOAT
            );
        """)
        cur.execute("""
            COPY INTO mortgage_rates_staging
            FROM @mortgage_data_stage/mortgage_rate_quarterly.csv
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
        """)

        # ✅ Load One-Family Housing Starts data
        cur.execute("""
            CREATE OR REPLACE TABLE housing_starts_one_family_q (
                Period STRING,
                Total INT,
                Purpose_of_Construction_Built_for_Sale_Total INT,
                Purpose_of_Construction_Built_for_Sale_Fee_Simple INT,
                Purpose_of_Construction_Contractor_Built INT,
                Purpose_of_Construction_Owner_Built INT,
                Design_Type_Detached INT,
                Design_Type_Attached INT,
                Square_Feet_Floor_Area_Median FLOAT,
                Square_Feet_Floor_Area_Average FLOAT
            );
        """)
        cur.execute("""
            COPY INTO housing_starts_one_family_q
            FROM @starts_data_stage/one_family_units_quarterly.csv
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
        """)

        # ✅ Load Multi-Unit Housing Starts data
        cur.execute("""
            CREATE OR REPLACE TABLE housing_starts_multi_units_q (
                Period STRING,
                Total_Units_in_Buildings_2plus INT,
                Purpose_of_Construction_For_Sale INT,
                Purpose_of_Construction_For_Rent INT,
                Number_of_Units_2_to_4 INT,
                Number_of_Units_5_to_9 INT,
                Number_of_Units_10_to_19 INT,
                Number_of_Units_20_or_More INT,
                Square_Feet_Per_Unit_Median FLOAT,
                Square_Feet_Per_Unit_Average FLOAT
            );
        """)
        cur.execute("""
            COPY INTO housing_starts_multi_units_q
            FROM @starts_data_stage/multi_units_buildings_quarterly.csv
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
        """)

        # ✅ Load Unemployment Rate data
        cur.execute("""
            CREATE OR REPLACE TABLE unemployment_rate_staging (
                Period STRING,
                Unemployment_Rate FLOAT
            );
        """)
        cur.execute("""
            COPY INTO unemployment_rate_staging
            FROM @cpi_unemp_data_stage/unemployment_rate_quarterly.csv
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
        """)

        # ✅ Load CPI data
        cur.execute("""
            CREATE OR REPLACE TABLE cpi_staging (
                Period STRING,
                Consumer_Price_Index FLOAT
            );
        """)
        cur.execute("""
            COPY INTO cpi_staging
            FROM @cpi_unemp_data_stage/cpi_quarterly.csv
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
        """)

        conn.commit()
        cur.close()
        print("✅ All staged data loaded successfully into Snowflake tables.")

    # 🔹 Step 4: Read and Process Data
    def process(self):
        """Main method to execute the data processing workflow with a single Snowflake connection."""

        # ✅ Use existing database and schema
        self.schema_manager.use_existing_schema()

        # ✅ Establish a single Snowflake connection
        conn = self.sf_connector.get_connection()

        try:
            # ✅ Step 1: Download raw files
            self.download_data_from_github()

            # ✅ Step 2: Upload raw data to Snowflake stage (using the open connection)
            self.upload_data_to_snowflake_stage(conn)  # FIXED: Passing conn

            # ✅ Step 3: Load staged data into Snowflake tables (using the same connection)
            self.load_staged_data_to_snowflake_tables(conn)  # FIXED: Passing conn

            # ✅ Step 4: Read and clean data
            prices_df, mortgage_df, starts_df = self.read_raw_data()
            prices_df, mortgage_df, starts_df = self.clean_data(prices_df, mortgage_df, starts_df)

            # ✅ Step 5: Compute derived metrics
            metrics_df = self.calculate_derived_metrics(prices_df, mortgage_df)

            # ✅ Step 6: Prepare Data for Snowflake Warehouse
            warehouse_data = self.prepare_for_warehouse(metrics_df)

            # ✅ Step 7: Load Processed Data to Snowflake Warehouse
            self.load_to_warehouse(warehouse_data)

        finally:
            # ✅ Close connection only after everything is done
            conn.close()
            print("✅ Snowflake connection closed after successful processing.")

if __name__ == "__main__":
    processor = HousingDataProcessor()
    processor.process()