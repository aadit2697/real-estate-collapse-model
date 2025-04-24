import pandas as pd
import os

# Load the Excel file
file_path = "https://raw.githubusercontent.com/aadit2697/real-estate-collapse-model/kapil_dev/data/raw/quar_starts_purpose_cust.xlsx"  # actual path
sheet_name = "StartsUSIntentQ" 

# Read the data while skipping unnecessary metadata rows
df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=5)

# Drop fully empty columns (if any)
df.dropna(axis=1, how='all', inplace=True)

# Remove the first two rows (redundant headers)
df_cleaned = df.iloc[2:].reset_index(drop=True)

##################-------------- ONE FAMILY UNITS--------------------#######################

# Extract "One-Family Units" table
one_family_cols = [
    "Unnamed: 0",  # Period
    "Unnamed: 1",  # Total
    "Purpose of construction", "Unnamed: 3", "Unnamed: 4", "Unnamed: 5",  # Built for sale categories
    "Design type", "Unnamed: 7", "Square feet of floor area", "Unnamed: 9"  # Design type and area
]

one_family_df = df_cleaned[one_family_cols].copy()

# Rename columns
one_family_df.columns = [
    "Period",
    "Total",
    "Purpose_of_Construction_Built_for_Sale_Total",
    "Purpose_of_Construction_Built_for_Sale_Fee_Simple",
    "Purpose_of_Construction_Contractor_Built",
    "Purpose_of_Construction_Owner_Built",
    "Design_Type_Detached",
    "Design_Type_Attached",
    "Square_Feet_Floor_Area_Median",
    "Square_Feet_Floor_Area_Average"
]

one_family_df = one_family_df.loc[:198].reset_index(drop=True)

# Convert Period column to quarterly format
one_family_df["Period"] = pd.PeriodIndex(one_family_df["Period"], freq="Q")

# Convert numerical columns to appropriate data types
numerical_columns = [
    "Total",
    "Purpose_of_Construction_Built_for_Sale_Total",
    "Purpose_of_Construction_Built_for_Sale_Fee_Simple",
    "Purpose_of_Construction_Contractor_Built",
    "Purpose_of_Construction_Owner_Built",
    "Design_Type_Detached",
    "Design_Type_Attached"
]

# Convert to integer while allowing NaNs
one_family_df[numerical_columns] = one_family_df[numerical_columns].apply(pd.to_numeric, errors="coerce").astype("Int64")

# Convert Square Feet columns to float
one_family_df["Square_Feet_Floor_Area_Median"] = pd.to_numeric(one_family_df["Square_Feet_Floor_Area_Median"], errors="coerce").astype("float64")
one_family_df["Square_Feet_Floor_Area_Average"] = pd.to_numeric(one_family_df["Square_Feet_Floor_Area_Average"], errors="coerce").astype("float64")

# Display updated dataframe info
print(one_family_df.info())

# Define the processed data path
processed_data_path = "data/processed/"

# ✅ Ensure the directory exists (creates it if missing)
os.makedirs(processed_data_path, exist_ok=True)

# Save one family units data as a csv in the processed data folder
one_family_df.to_csv(f"{processed_data_path}one_family_units.csv", index=False)

##################--------------MULTI UNITS BUILDINGS--------------------#######################

# Extract "Units in Buildings with 2+ Units" table
multi_units_cols = [
    "Unnamed: 0",  # Period
    "Unnamed: 10",  # Total Units in 2+ Buildings
    "Purpose of construction.1", "Unnamed: 12",  # Purpose of construction (For sale, For rent)
    "Number of units in building", "Unnamed: 14", "Unnamed: 15", "Unnamed: 16",  # Number of units breakdown
    "Square feet per unit", "Unnamed: 18"  # Square feet per unit (Median, Average)
]

multi_units_df = df_cleaned[multi_units_cols].copy()

# Rename columns
multi_units_df.columns = [
    "Period",
    "Total_Units_in_Buildings_2+",
    "Purpose_of_Construction_For_Sale",
    "Purpose_of_Construction_For_Rent",
    "Number_of_Units_2_to_4",
    "Number_of_Units_5_to_9",
    "Number_of_Units_10_to_19",
    "Number_of_Units_20_or_More",
    "Square_Feet_Per_Unit_Median",
    "Square_Feet_Per_Unit_Average"
]

multi_units_df = multi_units_df.loc[:198].reset_index(drop=True)

# Convert Period column to quarterly format
multi_units_df["Period"] = pd.PeriodIndex(multi_units_df["Period"], freq="Q")

# Convert numerical columns to appropriate data types
numerical_columns = [
    "Total_Units_in_Buildings_2+",
    "Purpose_of_Construction_For_Sale",
    "Purpose_of_Construction_For_Rent",
    "Number_of_Units_2_to_4",
    "Number_of_Units_5_to_9",
    "Number_of_Units_10_to_19",
    "Number_of_Units_20_or_More"
]

# Convert to integer while allowing NaNs
multi_units_df[numerical_columns] = multi_units_df[numerical_columns].apply(pd.to_numeric, errors="coerce").astype("Int64")

# Convert Square Feet columns to float
multi_units_df["Square_Feet_Per_Unit_Median"] = pd.to_numeric(multi_units_df["Square_Feet_Per_Unit_Median"], errors="coerce").astype("float64")
multi_units_df["Square_Feet_Per_Unit_Average"] = pd.to_numeric(multi_units_df["Square_Feet_Per_Unit_Average"], errors="coerce").astype("float64")

# Display updated dataframe info
print(multi_units_df.info())

# Save one family units data as a csv in the processed data folder
multi_units_df.to_csv(f"{processed_data_path}multi_units_buildings.csv", index=False)