
from pyspark.sql import SparkSession, types, functions as F
import os

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()


def process_csv_files(src_path, file_columns):
    """
    
    """
    for file in os.listdir(src_path):
        if file.endswith(".csv"):
            df=spark.read.option('header', 'true') \
            .csv(os.path.join(src_path, file))
            for col in df.columns:
                if col not in file_columns:
                    df=df.drop(col)
            for col in file_columns:
                if col not in df.columns:
                    df=df.withColumn(col, F.lit(None).cast(types.StringType()))
            
            df.write.option("header", "true") \
            .csv(f"{src_path}/processed/{file}")
            os.system(f"cd {src_path}/processed/{file} && cat p*.csv > {file}")


def write_parquet(src_file: str, schema, cols_to_select, dest_name):
    """
    
    """

    path_list=[]
    df= spark.createDataFrame([], schema=schema)
    for root, dirs, files in os.walk(src_file):
        for file in files:
            if (file.startswith("1") or file.startswith("2")) and file.endswith('.csv'):
                path_list.append(os.path.join(root, file))
            else:
                continue

    #Read each csv file in list above and make one file to be saved as parquet format
    for file in path_list:
        tmp_df = spark.read.option('header', 'true').csv(file).select(cols_to_select)
        if 'person' in src_file or 'vehicle' in src_file:
            year = ''.join(char for char in file if char.isdigit())
            tmp_df = tmp_df.withColumn("YEAR", F.lit(year[:4])) 
        df=df.unionAll(tmp_df)
    df.write.parquet(f"{dest_name}.parquet", mode='overwrite')
    os.system(f"cd {src_file} && cd .. && rm -r processed")  #deletes the spark csv files and not the original downloaded files

accident_columns = ["ST_CASE", "STATE", "VE_TOTAL", "VE_FORMS", "PVH_INVL", "PEDS", "PERSONS", "PERMVIT", "PERNOTMVIT", "COUNTY", "CITY", "DAY", "MONTH", "YEAR", "DAY_WEEK", "HOUR", "MINUTE", "NHS", "ROUTE", "CL_TWAY", "RUR_URB", "LAND_USE", "ROAD_FNC", "REL_ROAD", "REL_JUNC", "RELJCT2", "LATITUDE", "LONGITUD", "HARM_EV", "MAN_COLL", "TYP_INT", "LGT_COND", "WEATHER", "SCH_BUS", "NOT_HOUR", "NOT_MIN", "ARR_HOUR", "ARR_MIN", "HOSP_HR", "HOSP_MIN", "FATALS", "DRUNK_DR"]

accident_schema = types.StructType([
    types.StructField("ST_CASE", types.StringType(), True),
    types.StructField("YEAR", types.StringType(), True),
    types.StructField("MONTH", types.StringType(), True),
    types.StructField("DAY", types.StringType(), True),
    types.StructField("DAY_WEEK", types.StringType(), True),
    types.StructField("HOUR", types.StringType(), True),
    types.StructField("MINUTE", types.StringType(), True),
    types.StructField("STATE", types.StringType(), True),
    types.StructField("CITY", types.StringType(), True),
    types.StructField("COUNTY", types.StringType(), True),
    types.StructField("NOT_HOUR", types.StringType(), True),
    types.StructField("NOT_MIN", types.StringType(), True),
    types.StructField("ARR_HOUR", types.StringType(), True),
    types.StructField("ARR_MIN", types.StringType(), True),
    types.StructField("HOSP_HR", types.StringType(), True),
    types.StructField("HOSP_MIN", types.StringType(), True),
    types.StructField("PERSONS", types.StringType(), True),
    types.StructField("FATALS", types.StringType(), True),
    types.StructField("DRUNK_DR", types.StringType(), True),
    types.StructField("LGT_COND", types.StringType(), True),
    types.StructField("WEATHER", types.StringType(), True),
    types.StructField("VE_FORMS", types.StringType(), True),
    types.StructField("VE_TOTALS", types.StringType(), True),
    types.StructField("PERMVIT", types.StringType(), True),
    types.StructField("PERNOTMVIT", types.StringType(), True),
    types.StructField("PEDS", types.StringType(), True),
    types.StructField("PVH_INVL", types.StringType(), True),
    types.StructField("NHS", types.StringType(), True),
    types.StructField("ROUTE", types.StringType(), True),
    types.StructField("CL_TWAY", types.StringType(), True),
    types.StructField("RUR_URB", types.StringType(), True),
    types.StructField("LAND_USE", types.StringType(), True),
    types.StructField("ROAD_FNC", types.StringType(), True),
    types.StructField("REL_ROAD", types.StringType(), True),
    types.StructField("REL_JUNC", types.StringType(), True),
    types.StructField("RELJCT2", types.StringType(), True),
    types.StructField("HARM_EV", types.StringType(), True),
    types.StructField("MAN_COLL", types.StringType(), True),
    types.StructField("TYP_INT", types.StringType(), True),
    types.StructField("SCH_BUS", types.StringType(), True),
    types.StructField("LATITUDE", types.StringType(), True),
    types.StructField("LONGITUD", types.StringType(), True)
])
cols_to_select = ["ST_CASE", "YEAR", "MONTH", "DAY", "DAY_WEEK", "HOUR", "MINUTE", "STATE", "CITY", "COUNTY", "NOT_HOUR", "NOT_MIN", "ARR_HOUR", "ARR_MIN", "HOSP_HR", "HOSP_MIN", "PERSONS", "FATALS", "DRUNK_DR", "LGT_COND", "WEATHER", "VE_FORMS", "VE_TOTAL", "PERMVIT", "PERNOTMVIT", "PEDS", "PVH_INVL", "NHS", "ROUTE", "CL_TWAY", "RUR_URB", "LAND_USE", "ROAD_FNC", "REL_ROAD", "REL_JUNC", "RELJCT2", "HARM_EV", "MAN_COLL", "TYP_INT", "SCH_BUS", "LATITUDE", "LONGITUD"]


####################################################################################################################################################################
"""
FOR ACCIDENT FILE
"""
df = spark.read.option('header', 'true').parquet('./local/accident.parquet')

#Removes the headers that were added in the UnionAll from the df
df = df.filter(df.ST_CASE != 'ST_CASE')

def states(state):
    
    dict_states = {
        '1': 'Alabama', '2': 'Alaska', '4': 'Arizona', '5': 'Arkansas', '6': 'California', '8': 'Colorado', 
        '9': 'Connecticut', '10': 'Delaware', '11': 'District of Columbia', '12': 'Florida','13': 'Georgia', 
        '15': 'Hawaii', '16': 'Idaho', '17': 'Illinois', '18': 'Indiana', '19': 'Iowa','20': 'Kansas', 
        '21': 'Kentucky', '22': 'Louisiana', '23': 'Maine', '24': 'Maryland', '25': 'Massachusetts', 
        '26': 'Michigan', '27': 'Minnesota', '28': 'Mississippi', '29': 'Missouri','30': 'Montana', 
        '31': 'Nebraska', '32': 'Nevada', '33': 'New Hamsphire', '34': 'New Jersey','35': 'New Mexico', 
        '36': 'New York', '37': 'North Carolina', '38': 'North Dakota', '39': 'Ohio', '40': 'Oklahoma', 
        '41': 'Oregon', '42': 'Pennsylvania', '43': 'Puerto Rico', '44': 'Rhode Island', '45': 'South Carolina', 
        '46': 'South Dakota', '47': 'Tennessee', '48': 'Texas','49': 'Utah', '50': 'Vermont', 
        '51': 'Virgin Islands', '52': 'Virginia', '53': 'Washington', '54': 'West Virginia', 
        '55': 'Wisconsin', '56': 'Wyoming'
    }

    if state in dict_states:
        return dict_states[state]
    else:
        return "Unknown"
    
states_udf = F.udf(states, returnType=types.StringType())
#State Column
df = df.withColumn('STATE', states_udf(df.STATE))

df.select('STATE').distinct().show()


####################################################################################################################################################################
"""
FOR PERSONS FILES
"""

person_columns = ['AGE', 'SEX', 'RACE', 'MAKE', "MOD_YEAR", 'DRINKING', 'DRUGS', 'INJ_SEV', 'VEH_NO', 'PER_NO', 'ST_CASE',]

person_schema = types.StructType([
    types.StructField("AGE", types.StringType(), True),
    types.StructField("SEX", types.StringType(), True),
    types.StructField("RACE", types.StringType(), True),
    types.StructField("MAKE", types.StringType(), True),
    types.StructField("MOD_YEAR", types.StringType(), True),
    types.StructField("DRINKING", types.StringType(), True),
    types.StructField("DRUGS", types.StringType(), True),
    types.StructField("INJ_SEV", types.StringType(), True),
    types.StructField("VEH_NO", types.StringType(), True),
    types.StructField("PER_NO", types.StringType(), True),
    types.StructField("ST_CASE", types.StringType(), True),
    types.StructField("YEAR", types.StringType(), True),
])

# df_person = spark.read.option('header', 'true').parquet('all_person2')
# df_person = df_person.filter(df_person.AGE != 'AGE')
# df_person.show()


####################################################################################################################################################################
"""
FOR VEHICLE FILES
"""

vehicle_columns = ['NUMOCCS', 'MAKE', "MOD_YEAR",'REG_STAT', 'L_STAT', 'DR_ZIP', 'DR_HGT', 'DR_WGT', 'DR_DRINK', 'DEATHS', 'VEH_NO', 'TRAV_SP', 'ST_CASE']
vehicle_schema = types.StructType([
    types.StructField("NUMOCCS", types.StringType(), True),
    types.StructField("MAKE", types.StringType(), True),
    types.StructField("MOD_YEAR", types.StringType(), True),
    types.StructField("REG_STAT", types.StringType(), True),
    types.StructField("L_STAT", types.StringType(), True),
    types.StructField("DR_ZIP", types.StringType(), True),
    types.StructField("DR_HGT", types.StringType(), True),
    types.StructField("DR_WGT", types.StringType(), True),
    types.StructField("DR_DRINK", types.StringType(), True),
    types.StructField("DEATHS", types.StringType(), True),
    types.StructField("VEH_NO", types.StringType(), True),
    types.StructField("TRAV_SP", types.StringType(), True),
    types.StructField("ST_CASE", types.StringType(), True),
    types.StructField("YEAR", types.StringType(), True)
])


# df_vehicle = spark.read.option('header', 'true').parquet('all_vehicle2')
# df_vehicle = df_vehicle.filter(df_vehicle.ST_CASE != 'ST_CASE')


# #YEAR and UNIQ_ID
# df_vehicle = df_vehicle.withColumn('DATE', df_vehicle.YEAR.cast(types.DateType()))
# df_vehicle = df_vehicle.withColumn('SUB', F.substring('YEAR', 3, 2))
# df_vehicle = df_vehicle.withColumn('UNIQ_ID', F.concat(F.col('ST_CASE'), (F.col('SUB'))))

# #License state Column
# df_vehicle = df_vehicle.withColumn('L_STATE', states_udf(df_vehicle.L_STAT))

