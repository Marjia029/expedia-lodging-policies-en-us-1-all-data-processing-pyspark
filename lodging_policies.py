import re
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import struct, col, udf, coalesce, lit, when
from pyspark.sql.types import StringType
import pyspark.sql.functions as F
import time

def initialize_spark():
    try:
        start_time = time.time()
        spark = SparkSession.builder \
            .appName("IcebergWrite") \
            .master("local[*]") \
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.local.type", "hadoop") \
            .config("spark.sql.catalog.local.warehouse", "warehouse") \
            .getOrCreate()
        
        end_time = time.time()
        print(f"Time to initialize Spark session: {end_time - start_time} seconds")

        return spark
    except Exception as e:
        print(f"Error initializing Spark session: {e}")
        return None


def read_json_file(spark, file_path):
    try:
        start_time = time.time()
        df = spark.read.json(file_path)
        print("Reading_data Done")

        end_time = time.time()
        print(f"Time to read JSON file: {end_time - start_time} seconds")

        return df
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        return None


def remove_html_tags_array(text_array):
    def remove_html_tags(text):
        return re.sub('<.*?>', '', text)
    return ' '.join([remove_html_tags(text) for text in text_array])


def get_country_code(country_name):
    country_code_mapping = {
        "Aland Islands": "AX",
        "Albania": "AL",
        "Algeria": "DZ",
        "American Samoa": "AS",
        "Andorra": "AD",
        "Angola": "AO",
        "Anguilla": "AI",
        "Antigua and Barbuda": "AG",
        "Argentina": "AR",
        "Armenia": "AM",
        "Aruba": "AW",
        "Australia": "AU",
        "Austria": "AT",
        "Azerbaijan": "AZ",
        "Bahamas": "BS",
        "Bahrain": "BH",
        "Bangladesh": "BD",
        "Barbados": "BB",
        "Belarus": "BY",
        "Belgium": "BE",
        "Belize": "BZ",
        "Benin": "BJ",
        "Bermuda": "BM",
        "Bhutan": "BT",
        "Bolivia": "BO",
        "Bonaire Saint Eustatius and Saba": "BQ",
        "Bosnia and Herzegovina": "BA",
        "Botswana": "BW",
        "Brazil": "BR",
        "British Virgin Islands": "VG",
        "Brunei": "BN",
        "Bulgaria": "BG",
        "Burkina Faso": "BF",
        "Burundi": "BI",
        "Cambodia": "KH",
        "Cameroon": "CM",
        "Canada": "CA",
        "Cape Verde": "CV",
        "Cayman Islands": "KY",
        "Chad": "TD",
        "Chile": "CL",
        "China": "CN",
        "Christmas Island": "CX",
        "Colombia": "CO",
        "Comoros": "KM",
        "Cook Islands": "CK",
        "Costa Rica": "CR",
        "Croatia": "HR",
        "Cuba": "CU",
        "Curacao": "CW",
        "Cyprus": "CY",
        "Czech Republic": "CZ",
        "Democratic Republic of the Congo": "CD",
        "Denmark": "DK",
        "Djibouti": "DJ",
        "Dominica": "DM",
        "Dominican Republic": "DO",
        "Ecuador": "EC",
        "Egypt": "EG",
        "El Salvador": "SV",
        "Equatorial Guinea": "GQ",
        "Eritrea": "ER",
        "Estonia": "EE",
        "Ethiopia": "ET",
        "Faroe Islands": "FO",
        "Fiji": "FJ",
        "Finland": "FI",
        "France": "FR",
        "French Guiana": "GF",
        "French Polynesia": "PF",
        "Gabon": "GA",
        "Gambia": "GM",
        "Georgia": "GE",
        "Germany": "DE",
        "Ghana": "GH",
        "Gibraltar": "GI",
        "Greece": "GR",
        "Greenland": "GL",
        "Grenada": "GD",
        "Guadeloupe": "GP",
        "Guam": "GU",
        "Guatemala": "GT",
        "Guernsey": "GG",
        "Guinea": "GN",
        "Guinea-Bissau": "GW",
        "Guyana": "GY",
        "Haiti": "HT",
        "Honduras": "HN",
        "Hong Kong": "HK",
        "Hungary": "HU",
        "Iceland": "IS",
        "India": "IN",
        "Indonesia": "ID",
        "Iraq": "IQ",
        "Ireland": "IE",
        "Isle of Man": "IM",
        "Israel": "IL",
        "Italy": "IT",
        "Ivory Coast": "CI",
        "Jamaica": "JM",
        "Japan": "JP",
        "Jersey": "JE",
        "Jordan": "JO",
        "Kazakhstan": "KZ",
        "Kenya": "KE",
        "Kiribati": "KI",
        "Kuwait": "KW",
        "Kyrgyzstan": "KG",
        "Laos": "LA",
        "Latvia": "LV",
        "Lebanon": "LB",
        "Lesotho": "LS",
        "Liberia": "LR",
        "Liechtenstein": "LI",
        "Lithuania": "LT",
        "Luxembourg": "LU",
        "Macao": "MO",
        "Macedonia": "MK",
        "Madagascar": "MG",
        "Malawi": "MW",
        "Malaysia": "MY",
        "Maldives": "MV",
        "Mali": "ML",
        "Malta": "MT",
        "Martinique": "MQ",
        "Mauritania": "MR",
        "Mauritius": "MU",
        "Mayotte": "YT",
        "Mexico": "MX",
        "Micronesia": "FM",
        "Moldova": "MD",
        "Monaco": "MC",
        "Mongolia": "MN",
        "Montenegro": "ME",
        "Montserrat": "MS",
        "Morocco": "MA",
        "Mozambique": "MZ",
        "Myanmar": "MM",
        "Namibia": "NA",
        "Nepal": "NP",
        "Netherlands": "NL",
        "New Caledonia": "NC",
        "New Zealand": "NZ",
        "Nicaragua": "NI",
        "Niger": "NE",
        "Nigeria": "NG",
        "Niue": "NU",
        "Norfolk Island": "NF",
        "Northern Mariana Islands": "MP",
        "Norway": "NO",
        "Oman": "OM",
        "Pakistan": "PK",
        "Palau": "PW",
        "Palestinian Territory": "PS",
        "Panama": "PA",
        "Papua New Guinea": "PG",
        "Paraguay": "PY",
        "Peru": "PE",
        "Philippines": "PH",
        "Poland": "PL",
        "Portugal": "PT",
        "Puerto Rico": "PR",
        "Qatar": "QA",
        "Republic of the Congo": "CG",
        "Reunion": "RE",
        "Romania": "RO",
        "Rwanda": "RW",
        "Saint Barthelemy": "BL",
        "Saint Kitts and Nevis": "KN",
        "Saint Lucia": "LC",
        "Saint Martin": "MF",
        "Saint Pierre and Miquelon": "PM",
        "Saint Vincent and the Grenadines": "VC",
        "Samoa": "WS",
        "San Marino": "SM",
        "Sao Tome and Principe": "ST",
        "Saudi Arabia": "SA",
        "Senegal": "SN",
        "Serbia": "RS",
        "Seychelles": "SC",
        "Sierra Leone": "SL",
        "Singapore": "SG",
        "Sint Maarten": "SX",
        "Slovakia": "SK",
        "Slovenia": "SI",
        "Solomon Islands": "SB",
        "South Africa": "ZA",
        "South Korea": "KR",
        "Spain": "ES",
        "Sri Lanka": "LK",
        "Sudan": "SD",
        "Suriname": "SR",
        "Svalbard and Jan Mayen": "SJ",
        "Swaziland": "SZ",
        "Sweden": "SE",
        "Switzerland": "CH",
        "Taiwan": "TW",
        "Tajikistan": "TJ",
        "Tanzania": "TZ",
        "Thailand": "TH",
        "Togo": "TG",
        "Tonga": "TO",
        "Trinidad and Tobago": "TT",
        "Tunisia": "TN",
        "Turkey": "TR",
        "Turkmenistan": "TM",
        "Turks and Caicos Islands": "TC",
        "U.S. Virgin Islands": "VI",
        "Uganda": "UG",
        "United Arab Emirates": "AE",
        "United Kingdom": "UK",
        "United States": "US",
        "United States Minor Outlying Islands": "UM",
        "Uruguay": "UY",
        "Uzbekistan": "UZ",
        "Vanuatu": "VU",
        "Vietnam": "VN",
        "Wallis and Futuna": "WF",
        "Zambia": "ZM",
        "Zimbabwe": "ZW",
        "" :""
        # Add more mappings as needed
    }
    return country_code_mapping.get(country_name, "Unknown")


def transform_dataframe(df, remove_html_tags_udf, get_country_code_udf):
    try:
        start_time = time.time()
        
        transformed_df = df.select(
            when(col("checkInStartTime").isNull(), lit("")).otherwise(col("checkInStartTime")).alias("check_in"),
            when(col("checkOutTime").isNull(), lit("")).otherwise(col("checkOutTime")).alias("check_out"),
            struct(
                remove_html_tags_udf(coalesce(col("petPolicy"), lit([""]))).alias("pet_policy"),
                remove_html_tags_udf(coalesce(col("childrenAndExtraBedPolicy"), lit([""]))).alias("child_policy")
            ).alias("policy"),
            get_country_code_udf(coalesce(col("country"), lit(""))).alias("country_code")
        )
        
        end_time = time.time()
        print(f"Time to transform DataFrame: {end_time - start_time} seconds")
        
        return transformed_df
    except Exception as e:
        print(f"Error transforming DataFrame: {e}")
        return None

def write_to_iceberg(transformed_df,spark):
    try:
        start_time = time.time()

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS local.default.lodging_policies (
                check_in string,
                check_out string,
                policy struct<pet_policy: string, child_policy: string>,
                country_code  string)
            USING iceberg
            PARTITIONED BY (country_code)
            OPTIONS ('format-version'='2')
        """)

        transformed_df.distinct().writeTo("local.default.lodging_policies") \
            .partitionedBy("country_code") \
            .tableProperty("write.format.default", "parquet") \
            .tableProperty("write.parquet.compression-codec", "snappy") \
            .append()
        
        end_time = time.time()
        print(f"Time to write to Iceberg: {end_time - start_time} seconds")

    except Exception as e:
        print(f"Error writing to Iceberg: {e}")
            
    
def main():
    start_time = time.time()

    #Start the session
    spark = initialize_spark()
    if not spark:
        return
    
    # Read JSON data from the file and transform it into a DataFrame
    df = read_json_file(spark, "input/expedia-lodging-policies-en_us-1-all.jsonl")
    if df is None:
        print("Could not read JSON file")
        return
    #Register the functions as UDF
    remove_html_tags_udf = udf(remove_html_tags_array, StringType())
    get_country_code_udf = udf(get_country_code, StringType())

    # Transform the DataFrame
    transformed_df = transform_dataframe(df, remove_html_tags_udf, get_country_code_udf)
    if transformed_df is None:
        return

    transformed_df.show(10, truncate = False)
    transformed_df.printSchema()
    # Write the transformed DataFrame to Iceberg
    write_to_iceberg(transformed_df,spark)
    current_db = spark.catalog.currentDatabase()
    print(f"Current database: {current_db}")

    spark.sql("SELECT count(*) as total_records, country_code FROM local.default.lodging_policies GROUP BY country_code ORDER BY total_records DESC").show(50)
    spark.sql("SELECT * FROM local.default.lodging_policies ORDER BY country_code ASC").show(50)
    # Query the partitions metadata table
    spark.sql("SELECT partition, record_count FROM local.default.lodging_policies.partitions").show(50,truncate=False)

    # Execute SQL to count the total number of rows in the table
    result = spark.sql("SELECT COUNT(*) AS total_rows FROM local.default.lodging_policies").collect()
    total_rows = result[0]['total_rows']

    # Print the total number of rows
    print(f"Total rows: {total_rows}")

    # Stop the Spark session
    spark.stop()

    end_time = time.time()

    print(f"Time to execute file: {end_time - start_time} seconds")

if __name__ == "__main__":
    main()