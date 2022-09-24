import argparse
from pyspark.sql import SparkSession, types, functions as F


parser = argparse.ArgumentParser()

parser.add_argument('--input_accident', required=True)
parser.add_argument('--input_person', required=True)
parser.add_argument('--input_vehicle', required=True)

args = parser.parse_args()

input_accident = args.input_accident
input_person = args.input_person
input_vehicle = args.input_vehicle


spark = SparkSession.builder \
    .master('yarn') \
    .appName('test') \
    .getOrCreate()

bucket = '<YOUR TEMPORARY BUCKET>'
spark.conf.set('temporaryGcsBucket', bucket)

accident_columns = ["ST_CASE", "YEAR", "MONTH", "DAY", "DAY_WEEK", "HOUR", "MINUTE", "STATE", "CITY", "COUNTY", "NOT_HOUR", "NOT_MIN", "ARR_HOUR", "ARR_MIN", "HOSP_HR", "HOSP_MN", "PERSONS", "FATALS", "DRUNK_DR", "LGT_COND", "WEATHER","VE_FORMS", "VE_TOTAL", "PERMVIT", "PERNOTMVIT", "PEDS", "PVH_INVL", "NHS", "ROUTE", "CL_TWAY", "RUR_URB", "LAND_USE", "ROAD_FNC", "HARM_EV", "MAN_COLL", "TYP_INT", "REL_JUNC", "RELJCT2", "REL_ROAD", "SCH_BUS",  "LATITUDE", "LONGITUD"]

person_columns = ['AGE', 'SEX', 'RACE', 'MAKE', "MOD_YEAR", 'DRINKING', 'DRUGS', 'INJ_SEV', 'VEH_NO', 'PER_NO', 'ST_CASE', 'YEAR']

vehicle_columns = ['NUMOCCS', 'MAKE', "MOD_YEAR",'REG_STAT', 'L_STAT', 'DR_ZIP', 'DR_HGT', 'DR_WGT', 'DR_DRINK', 'DEATHS', 'VEH_NO', 'TRAV_SP', 'ST_CASE', 'YEAR']

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
    types.StructField("HOSP_MN", types.StringType(), True),
    types.StructField("PERSONS", types.StringType(), True),
    types.StructField("FATALS", types.StringType(), True),
    types.StructField("DRUNK_DR", types.StringType(), True),
    types.StructField("LGT_COND", types.StringType(), True),
    types.StructField("WEATHER", types.StringType(), True),
    types.StructField("VE_FORMS", types.StringType(), True),
    types.StructField("VE_TOTAL", types.StringType(), True),
    types.StructField("PERMVIT", types.StringType(), True),
    types.StructField("PERNOTMVIT", types.StringType(), True),
    types.StructField("PEDS", types.StringType(), True),
    types.StructField("PVH_INVL", types.StringType(), True),
    types.StructField("NHS", types.StringType(), True),
    types.StructField("ROUTE", types.StringType(), True),
    types.StructField('ROAD_FNC', types.StringType(), True),
    types.StructField("LAND_USE", types.StringType(), True),
    types.StructField("RUR_URB", types.StringType(), True),
    types.StructField("CL_TWAY", types.StringType(), True),
    types.StructField("REL_JUNC", types.StringType(), True),
    types.StructField("RELJCT2", types.StringType(), True),
    types.StructField("REL_ROAD", types.StringType(), True),
    types.StructField("HARM_EV", types.StringType(), True),
    types.StructField("MAN_COLL", types.StringType(), True),
    types.StructField("TYP_INT", types.StringType(), True),
    types.StructField("SCH_BUS", types.IntegerType(), True),
    types.StructField("LATITUDE", types.StringType(), True),
    types.StructField("LONGITUD", types.StringType(), True)
])

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


def create_df_from_gcs(file_input_path, file_type, file_cols_to_select, file_schema):

    df= spark.createDataFrame([], schema=file_schema)
    year = 1975

    for _ in range(46):
        tmp_df = spark.read.option('header', 'true').parquet(f'{file_input_path}/{year}_{file_type}.parquet')
        for col_name in tmp_df.columns:
            if col_name not in file_cols_to_select:
                tmp_df=tmp_df.drop(col_name)
        for col_name in file_cols_to_select:
            if col_name == 'YEAR' and col_name not in tmp_df.columns:
                tmp_df = tmp_df.withColumn(col_name, F.lit(str(year)))
            elif col_name not in tmp_df.columns:
                tmp_df=tmp_df.withColumn(col_name, F.lit(None).cast(types.StringType()))
        tmp_df = tmp_df.select([F.col(column).cast(types.StringType()) for column in file_cols_to_select])
        df=df.unionByName(tmp_df)
        year += 1
    return df


df_accident = create_df_from_gcs(file_input_path=input_accident, file_type='accident', file_cols_to_select=accident_columns, file_schema=accident_schema)
df_person = create_df_from_gcs(file_input_path=input_person, file_type='person', file_cols_to_select=person_columns, file_schema=person_schema)
df_vehicle = create_df_from_gcs(file_input_path=input_vehicle, file_type='vehicle', file_cols_to_select=vehicle_columns, file_schema=vehicle_schema)

df_accident = df_accident.repartition(100)
df_person = df_person.repartition(100)
df_vehicle = df_vehicle.repartition(100)


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
        '51': 'Virginia', '52': 'Virgin Islands', '53': 'Washington', '54': 'West Virginia', 
        '55': 'Wisconsin', '56': 'Wyoming'
    }

    if state in dict_states:
        return dict_states[state]
    else:
        return "Unknown"
    
states_udf = F.udf(states, returnType=types.StringType())

df_accident = df_accident.filter(df_accident.STATE != 'STATE')

#Add Year Column
df_accident = df_accident.withColumn("YEAR", F.when(F.length("YEAR") < 4, F.concat(F.lit('19'), F.col('YEAR'))).otherwise(df_accident.YEAR)) \
    .withColumn("DATE", F.concat_ws('-', F.col('YEAR'), F.col('MONTH'), F.col('DAY')).cast(types.DateType()))
df_accident = df_accident.withColumn('Y_DATE', df_accident.YEAR.cast(types.DateType()))
#Uniq_ID
df_accident = df_accident.withColumn('SUB', F.substring('YEAR', 3, 2))
df_accident = df_accident.withColumn('UNIQ_ID', F.concat(F.col('ST_CASE'), F.col('SUB')))

#State Column
df_accident = df_accident.withColumn('STATE', states_udf(df_accident.STATE))

#Day of Week
df_accident = df_accident.withColumn('DAY_OF_WEEK', F.when(F.col('DAY_WEEK') == '1', 'Sunday') \
                   .when(F.col('DAY_WEEK') == '2', 'Monday') \
                   .when(F.col('DAY_WEEK') == '3', 'Tuesday') \
                   .when(F.col('DAY_WEEK') == '4', 'Wednesday') \
                   .when(F.col('DAY_WEEK') == '5', 'Thursday') \
                   .when(F.col('DAY_WEEK') == '6', 'Friday') \
                   .when(F.col('DAY_WEEK') == '7', 'Saturday') \
                    .when(F.col('DAY_WEEK') == '9', 'Unknown') \
                   .otherwise(df_accident.DAY_WEEK))
                
#Light Conditions
df_accident = df_accident.withColumn('LIGHT_CONDITION', F.when(F.col('LGT_COND') == '1', 'Daylight') \
                   .when((F.col('LGT_COND') == '2') & (F.year(df_accident.Y_DATE) < 2009), 'Dark') \
                   .when((F.col('LGT_COND') == '2') & (F.year(df_accident.Y_DATE) >= 2009), 'Dark-Not Lighted') \
                   .when((F.col('LGT_COND') == '3') & (F.year(df_accident.Y_DATE) <= 2009), 'Dark but Lighted') \
                   .when((F.col('LGT_COND') == '3') & (F.year(df_accident.Y_DATE) >= 2010), 'Dark-Lighted') \
                   .when(F.col('LGT_COND') == '4', 'Dawn') \
                   .when(F.col('LGT_COND') == '5', 'Dusk') \
                   .when((F.col('LGT_COND') == '6') & ((F.year(df_accident.Y_DATE) <= 1979) | (F.year(df_accident.Y_DATE) >= 2009)),
                                                       'Dark-Unknown Lighting') \
                   .when((F.col('LGT_COND') == '7') & (F.year(df_accident.Y_DATE) >= 2009), 'Other') \
                   .when(F.col('LGT_COND') == '8', 'Other').when(F.col('LGT_COND') == '9', 'Unknown') \
                   .otherwise(df_accident.LGT_COND))
                
#Weather Conditions
df_accident = df_accident.withColumn('WEATHER_CONDITION', F.when(F.col('WEATHER') == '0', 'No Adverse Atmospheric Conditions') \
                  .when((F.col('WEATHER') == '1') & ((F.year(df_accident.Y_DATE) <= 1979) | (F.year(df_accident.Y_DATE) >= 2010)), 'Clear') \
                  .when((F.col('WEATHER') == '1') & ((F.year(df_accident.Y_DATE) == 1980) | (F.year(df_accident.Y_DATE) == 1981)), 'Normal') \
                  .when((F.col('WEATHER') == '1') & (F.year(df_accident.Y_DATE) <= 2006), 'No Adverse Atmospheric Conditions') \
                   .when((F.col('WEATHER') == '1') & ((F.year(df_accident.Y_DATE) >= 2007) | (F.year(df_accident.Y_DATE) <= 2009)), 'Clear/Cloud (No Adverse Conditions)') \
                  .when((F.col('WEATHER') == '2') & ((F.year(df_accident.Y_DATE) <= 1981) | (F.year(df_accident.Y_DATE) >= 2010)), 'Rain') \
                  .when((F.col('WEATHER') == '2') & (F.year(df_accident.Y_DATE) <= 2009), 'Rain(Mist)') \
                  .when((F.col('WEATHER') == '3') & (F.year(df_accident.Y_DATE) <= 1981), 'Sleet') \
                  .when((F.col('WEATHER') == '3') & (F.year(df_accident.Y_DATE) <= 2009), 'Sleet(Hail)') \
                  .when((F.col('WEATHER') == '3') & (F.year(df_accident.Y_DATE) >= 2010), 'Sleet, Hail (Freezing Rain or Drizzle)') \
                  .when((F.col('WEATHER') == '4'), 'Snow') \
                  .when((F.col('WEATHER') == '5') & (F.year(df_accident.Y_DATE) <= 2006), 'Fog') \
                  .when((F.col('WEATHER') == '5') & (F.year(df_accident.Y_DATE) >= 2007), 'Fog, Smog, Smoke') \
                  .when((F.col('WEATHER') == '6') & (F.year(df_accident.Y_DATE) <= 2006), 'Rain and Fog') \
                  .when((F.col('WEATHER') == '6') & (F.year(df_accident.Y_DATE) >= 2007), 'Severe Crosswinds') \
                  .when((F.col('WEATHER') == '7') & (F.year(df_accident.Y_DATE) <= 1979), 'Cloudy') \
                  .when((F.col('WEATHER') == '7') & (F.year(df_accident.Y_DATE) <= 2006), 'Sleet and Fog') \
                  .when((F.col('WEATHER') == '7') & (F.year(df_accident.Y_DATE) >= 2007), 'Blowing Sand, Soil, Dirt') \
                  .when((F.col('WEATHER') == '8') & (F.year(df_accident.Y_DATE) <= 2006), 'Other: Smog, Smoke, Blowing Sand or Dust') \
                  .when((F.col('WEATHER') == '8') & (F.year(df_accident.Y_DATE) >= 2007), 'Other') \
                  .when((F.col('WEATHER') == '10'), 'Cloudy') \
                  .when((F.col('WEATHER') == '11'), 'Blowing Snow') \
                  .when((F.col('WEATHER') == '12'), 'Freezing Rain or Drizzle') \
                  .when((F.col('WEATHER') == '98'), 'Not Reported') \
                  .when((F.col('WEATHER') == '9') | (F.col('WEATHER') == '99'), 'Unknown') \
                  .otherwise(df_accident.WEATHER))

#Concatting Several Columns including Times
df_accident = df_accident.withColumn("TIME_OF_ACCIDENT", F.date_format((F.concat_ws(':', F.col('HOUR'), F.col("MINUTE"))), 'hh:mm a')) \
    .withColumn("NOTIFICATION_TIME", F.date_format((F.concat_ws(':', F.col('NOT_HOUR'), F.col("NOT_MIN"))), 'hh:mm a')) \
    .withColumn("EMS_ARRIVAL_TIME", F.date_format((F.concat_ws(':', F.col('ARR_HOUR'), F.col("ARR_MIN"))), 'hh:mm a')) \
    .withColumn("HOSP_ARRIVAL_TIME", F.date_format((F.concat_ws(':', F.col('HOSP_HR'), F.col("HOSP_MN"))), 'hh:mm:a')) \
    .withColumn("ROUTE_CL_CONCAT", F.concat_ws("", F.col("ROUTE"), F.col('CL_TWAY'))) \
    .withColumn("REL_JUNC_CONCAT", F.concat_ws("", F.col("REL_JUNC"), F.col("RELJCT2"))) \
    .withColumn("RUR_OR_URB", F.concat_ws("", F.col("LAND_USE"), F.when(F.year(df_accident.Y_DATE) >= 1987, F.col("ROAD_FNC")), F.col("RUR_URB")))

#Rural or Urban
df_accident = df_accident.withColumn('RURAL_OR_URBAN', F.when((F.col('RUR_OR_URB') == '1') & (F.year(df_accident.Y_DATE) <= 1986), 'Urban') \
                   .when((F.col('RUR_OR_URB') == '2') & (F.year(df_accident.Y_DATE) <= 1986), 'Rural') \
                   .when((F.col('RUR_OR_URB') == '9') | (F.col('RUR_OR_URB') == '19') | (F.col('RUR_OR_URB') == '99'), \
                         'Unknown') \
                   .when((F.col('RUR_OR_URB') == '1') | (F.col('RUR_OR_URB') == '2') | (F.col('RUR_OR_URB') == '3') \
                         | (F.col('RUR_OR_URB') == '4') | (F.col('RUR_OR_URB') == '5') | (F.col('RUR_OR_URB') == '6') \
                         & (F.year(df_accident.Y_DATE) <= 2014), 'Rural') \
                   .when((F.col('RUR_OR_URB') == '11') | (F.col('RUR_OR_URB') == '12') | (F.col('RUR_OR_URB') == '13') \
                         | (F.col('RUR_OR_URB') == '14') | (F.col('RUR_OR_URB') == '15') | (F.col('RUR_OR_URB') == '16') \
                         & (F.year(df_accident.Y_DATE) <= 2014), 'Urban') \
                   .when((F.col('RUR_OR_URB') == '1') & (F.year(df_accident.Y_DATE) >= 2015), 'Rural') \
                   .when((F.col('RUR_OR_URB') == '2') & (F.year(df_accident.Y_DATE) >= 2015), 'Urban') \
                   .when((F.col('RUR_OR_URB') == '6') & (F.year(df_accident.Y_DATE) >= 2015), 'Trafficway Not in State Inventory') \
                   .when((F.col('RUR_OR_URB') == '8') & (F.year(df_accident.Y_DATE) >= 2015), 'Not Reported') \
                   .otherwise(df_accident.RUR_OR_URB))

#Type Intersection
df_accident = df_accident.withColumn('TYPE_INTERSECTION', F.when(F.col('TYP_INT') == '1', 'Not an Intersection') \
                  .when(F.col('TYP_INT') == '2', 'Four-Way Intersection') \
                  .when(F.col('TYP_INT') == '3', 'T-Intersection') \
                  .when(F.col('TYP_INT') == '4', 'Y-Intersection') \
                  .when(F.col('TYP_INT') == '5', 'Traffic Circle') \
                  .when(F.col('TYP_INT') == '6', 'Roundabout') \
                   .when(F.col('TYP_INT') == '7', 'Five-Point or More') \
                   .when((F.col('TYP_INT') == '8') | (F.col('TYP_INT') == '98'), 'Not Reported') \
                   .when((F.col('TYP_INT') == '9') | (F.col('TYP_INT') == '99'), 'Unknown') \
                   .when(F.col('TYP_INT') == '10', 'L-Intersection') \
                   .when(F.col('TYP_INT') == '11', 'Other Intersection Type') \
                   .otherwise(df_accident.TYP_INT))

#Manner of Collision
df_accident = df_accident.withColumn('MANNER_OF_COLLISION', F.when(F.col('MAN_COLL') == '0', 'Not Collision With Motor Vehicle in Transport') \
                  .when((F.col('MAN_COLL') == '1') & (F.year(df_accident.Y_DATE) <= 2001), 'Rear-end') \
                   .when((F.col('MAN_COLL') == '2') & (F.year(df_accident.Y_DATE) <= 2001), 'Head-on') \
                   .when((F.col('MAN_COLL') == '3') & (F.year(df_accident.Y_DATE) <= 2001), 'Rear-to-Rear') \
                   .when((F.col('MAN_COLL') == '4') & (F.year(df_accident.Y_DATE) <= 2001), 'Angle') \
                   .when((F.col('MAN_COLL') == '5') & (F.year(df_accident.Y_DATE) <= 2001), 'Sideswipe, Same Direction') \
                   .when((F.col('MAN_COLL') == '6') & (F.year(df_accident.Y_DATE) <= 2001), 'Sideswipe, Opposite Direction') \
                   .when((F.col('MAN_COLL') == '7') & (F.year(df_accident.Y_DATE) <= 2001), 'Sideswipe (May be Same or Opposite Directoin)') \
                   .when((F.col('MAN_COLL') == '9') & (F.year(df_accident.Y_DATE) <= 2001), 'Unknown') \
                   .when((F.col('MAN_COLL') == '1') & (F.year(df_accident.Y_DATE) >= 2002), 'Front-to-Rear') \
                   .when((F.col('MAN_COLL') == '2') & (F.year(df_accident.Y_DATE) >= 2002), 'Front-to-Front') \
                   .when((F.col('MAN_COLL') == '3') & (F.year(df_accident.Y_DATE) >= 2002), 'Angle-Front-to-Side, Same Direction') \
                   .when((F.col('MAN_COLL') == '4') & (F.year(df_accident.Y_DATE) >= 2002), 'Angle-Front-to-Side, Opposite Direction') \
                   .when((F.col('MAN_COLL') == '5') & (F.year(df_accident.Y_DATE) >= 2002), 'Angle-Front-to-Side, Right Angle') \
                   .when((F.col('MAN_COLL') == '6') & (F.year(df_accident.Y_DATE) >= 2002), 'Angle') \
                   .when((F.col('MAN_COLL') == '7') & (F.year(df_accident.Y_DATE) >= 2002), 'Sideswipe-Same Direction') \
                   .when((F.col('MAN_COLL') == '8') & (F.year(df_accident.Y_DATE) >= 2002), 'Sideswipe-Opposite Direction') \
                   .when((F.col('MAN_COLL') == '9') & (F.year(df_accident.Y_DATE) >= 2002), 'Rear-to-Side') \
                   .when((F.col('MAN_COLL') == '10') & (F.year(df_accident.Y_DATE) >= 2002), 'Rear-to-Rear') \
                   .when((F.col('MAN_COLL') == '11') & (F.year(df_accident.Y_DATE) >= 2002), 'Other (End-Swipes and Others)') \
                   .when((F.col('MAN_COLL') == '98') & (F.year(df_accident.Y_DATE) >= 2002), 'Not Reported') \
                   .when(F.col('MAN_COLL') == '99', 'Unknown') \
                   .otherwise(df_accident.MAN_COLL))

#Route/Trafficway
df_accident = df_accident.withColumn('ROUTE_TRAFFICWAY', F.when(F.col('ROUTE_CL_CONCAT') == '1', 'Interstate') \
                  .when((F.col('ROUTE_CL_CONCAT') == '2') & (F.year(df_accident.Y_DATE) <= 1980), 'Other Limited Access') \
                  .when((F.col('ROUTE_CL_CONCAT') == '3') & (F.year(df_accident.Y_DATE) <= 1980), 'Other U.S. Route') \
                  .when((F.col('ROUTE_CL_CONCAT') == '4') & (F.year(df_accident.Y_DATE) <= 1980), 'Other State Route') \
                  .when((F.col('ROUTE_CL_CONCAT') == '5') & (F.year(df_accident.Y_DATE) <= 1980), 'Other Major Artery') \
                  .when((F.col('ROUTE_CL_CONCAT') == '6') & (F.year(df_accident.Y_DATE) <= 1980), 'County Road') \
                  .when((F.col('ROUTE_CL_CONCAT') == '7') & (F.year(df_accident.Y_DATE) <= 1980), 'Local Street') \
                  .when((F.col('ROUTE_CL_CONCAT') == '8') & (F.year(df_accident.Y_DATE) <= 1980), 'Other Road') \
                  .when(F.col('ROUTE_CL_CONCAT') == '9', 'Unknown') \
                  .when((F.col('ROUTE_CL_CONCAT') == '2') & (F.year(df_accident.Y_DATE) <= 1986), 'Other U.S. Route') \
                  .when((F.col('ROUTE_CL_CONCAT') == '3') & (F.year(df_accident.Y_DATE) <= 1986), 'Other State Route') \
                  .when((F.col('ROUTE_CL_CONCAT') == '4') & (F.year(df_accident.Y_DATE) <= 1986), 'County Road') \
                  .when((F.col('ROUTE_CL_CONCAT') == '5') & (F.year(df_accident.Y_DATE) <= 1986), 'Local Street') \
                  .when((F.col('ROUTE_CL_CONCAT') == '8') & (F.year(df_accident.Y_DATE) <= 1986), 'Other Road') \
                  .when((F.col('ROUTE_CL_CONCAT') == '2') & (F.year(df_accident.Y_DATE) >= 1987), 'U.S. Highway') \
                  .when((F.col('ROUTE_CL_CONCAT') == '3') & (F.year(df_accident.Y_DATE) >= 1987), 'State Highway') \
                  .when((F.col('ROUTE_CL_CONCAT') == '4') & (F.year(df_accident.Y_DATE) >= 1987), 'County Road') \
                  .when((F.col('ROUTE_CL_CONCAT') == '5') & (F.year(df_accident.Y_DATE) >= 1987), 'Local Street-Township') \
                  .when((F.col('ROUTE_CL_CONCAT') == '6') & (F.year(df_accident.Y_DATE) >= 1987), 'Local Street-Municipality') \
                  .when((F.col('ROUTE_CL_CONCAT') == '7') & (F.year(df_accident.Y_DATE) >= 1987), 'Local Street-Frontage Road') \
                  .when((F.col('ROUTE_CL_CONCAT') == '8') & (F.year(df_accident.Y_DATE) >= 1987), 'Other') \
                  .otherwise(None))

#Relation to Jucntion
df_accident = df_accident.withColumn('RELATION_TO_JUNC', F.when(F.col('REL_JUNC_CONCAT') == '1', 'Non-Junction') \
                  .when((F.col('REL_JUNC_CONCAT') == '2') | (F.col('REL_JUNC_CONCAT') == '10'), 'Intersection') \
                  .when((F.col('REL_JUNC_CONCAT') == '3') | (F.col('REL_JUNC_CONCAT') == '11'), 'Intersection-Related') \
                  .when((F.col('REL_JUNC_CONCAT') == '4') & (F.year(df_accident.Y_DATE) <= 1990), 'Intersection Area') \
                  .when((F.col('REL_JUNC_CONCAT') == '4') & (F.year(df_accident.Y_DATE) <= 2009), 'Driveway, Alley Access etc.') \
                  .when((F.col('REL_JUNC_CONCAT') == '4') & (F.year(df_accident.Y_DATE) >= 2010), 'Driveway Access') \
                   .when((F.col('REL_JUNC_CONCAT') == '5') & (F.year(df_accident.Y_DATE) <= 1990), 'Driveway, Alley Access etc.') \
                   .when((F.col('REL_JUNC_CONCAT') == '5') & (F.year(df_accident.Y_DATE) >= 1991), 'Entrance/Exit Ramp Related') \
                   .when((F.col('REL_JUNC_CONCAT') == '6') & (F.year(df_accident.Y_DATE) <= 1990), 'Entrance/Exit Ramp') \
                   .when((F.col('REL_JUNC_CONCAT') == '6') & (F.year(df_accident.Y_DATE) >= 1991), 'Railway Grade Crossing') \
                   .when((F.col('REL_JUNC_CONCAT') == '7') & (F.year(df_accident.Y_DATE) <= 1990), 'Railway Grade Crossing') \
                   .when((F.col('REL_JUNC_CONCAT') == '7') & (F.year(df_accident.Y_DATE) >= 1991), 'Crossover Related') \
                   .when((F.col('REL_JUNC_CONCAT') == '8') & (F.year(df_accident.Y_DATE) <= 1990), 'Crossover Related') \
                   .when((F.col('REL_JUNC_CONCAT') == '8') & (F.year(df_accident.Y_DATE) >= 1991), 'Driveway Access Related') \
                   .when((F.col('REL_JUNC_CONCAT') == '9') | (F.col('REL_JUNC_CONCAT') == '99') | \
                        (F.col('REL_JUNC_CONCAT') == '19') , 'Unknown') \
                   .when(F.col('REL_JUNC_CONCAT') == '12', 'Driveway Access') \
                   .when(F.col('REL_JUNC_CONCAT') == '13', 'Entrance/Exit Ramp Related') \
                   .when(F.col('REL_JUNC_CONCAT') == '14', 'Crossover Related') \
                   .when(F.col('REL_JUNC_CONCAT') == '15', 'Other Location in Interchange') \
                   .when(F.col('REL_JUNC_CONCAT') == '16', 'Shared-Use Path Trail/Crossing') \
                  .when(F.col('REL_JUNC_CONCAT') == '17', 'Acceleration/Deceleration Lane') \
                  .when(F.col('REL_JUNC_CONCAT') == '18', 'Through Roadway') \
                  .when(F.col('REL_JUNC_CONCAT') == '19', 'Other Location Within Interchange Area') \
                  .when(F.col('REL_JUNC_CONCAT') == '20', 'Entrance/Exit Ramp') \
                  .when(((F.col('REL_JUNC_CONCAT') == '8') | (F.col('REL_JUNC_CONCAT') == '98')), 'Not Reported') \
                  .when(F.col('REL_JUNC_CONCAT') == '0', 'None') \
                  .otherwise(df_accident.REL_JUNC_CONCAT))

#Relation to Trafficway
df_accident = df_accident.withColumn("RELATION_TO_TRAFFICWAY", F.when(F.col('REL_ROAD') == '1', 'On Roadway') \
                  .when(F.col('REL_ROAD') == '2', 'On Shoulder') \
                  .when(F.col('REL_ROAD') == '3', 'On Median') \
                  .when(F.col('REL_ROAD') == '4', 'On Roadside') \
                  .when(F.col('REL_ROAD') == '5', 'Outside Trafficway/Outside Right-of-Way') \
                  .when(F.col('REL_ROAD') == '6', 'Off Roadway-Location Unknown') \
                  .when(F.col('REL_ROAD') == '7', 'In Parking Lane/Zone') \
                  .when(F.col('REL_ROAD') == '8', 'Gore') \
                  .when(F.col('REL_ROAD') == '10', 'Separator') \
                  .when(F.col('REL_ROAD') == '11', 'Continuous Left-Turn Lane') \
                    .when(F.col('REL_ROAD') == '12', 'Pedestrian Refuge Island or Traffic Island') \
                  .when(F.col('REL_ROAD') == '98', 'Not Reported') \
                  .when((F.col('REL_ROAD') == '9') | (F.col('REL_ROAD') == '99'), 'Unknown') \
                  .otherwise(df_accident.REL_ROAD))


###Temp View
df_accident.createOrReplaceTempView('all_accidents')
df_result = spark.sql("""
        SELECT
            CAST(UNIQ_ID AS STRING) AS Case_Number,
            YEAR(Y_DATE) AS Year,
            CAST(DATE AS DATE) AS Date,
            CAST(DAY_OF_WEEK AS STRING) AS Day_Of_Week,
            CAST(STATE AS STRING) AS State,
            CAST(CITY AS STRING) AS City,
            CAST(COUNTY AS STRING) AS County,
            CAST(TIME_OF_ACCIDENT AS STRING) AS Time_of_Accident,
            CAST(NOTIFICATION_TIME AS STRING) AS Notification_Time,
            CAST(EMS_ARRIVAL_TIME AS STRING) AS EMS_Arrival_Time,
            CAST(HOSP_ARRIVAL_TIME AS STRING) AS Hospital_Arrival_Time,
            CAST(PERSONS AS INTEGER) AS Num_Per_Inv,
            CAST(FATALS AS INTEGER) AS Fatals,
            CAST(DRUNK_DR AS INTEGER) AS Num_Drunk_Dr,
            CAST(VE_FORMS AS INTEGER) AS Num_Veh_Trans,
            CAST(VE_TOTAL AS INTEGER) AS Num_All_Veh,
            CAST(PERMVIT AS INTEGER) AS Num_Per_in_Veh,
            CAST(PERNOTMVIT AS INTEGER) AS Num_Per_Not_in_Veh,
            CAST(PEDS AS INTEGER) AS Num_Forms_Sub_Not_in_Veh,
            CAST(PVH_INVL AS INTEGER) AS Num_Parked_Veh,
            CAST(LIGHT_CONDITION AS STRING) AS Light_Conditions,
            CAST(WEATHER_CONDITION AS STRING) AS Weather_Conditions,
            CAST(NHS AS BOOLEAN) AS On_Nat_Highway_Sys,
            CAST(RURAL_OR_URBAN AS STRING) AS Rural_Or_Urban,
            CAST(ROUTE_TRAFFICWAY AS STRING) AS Route_Trafficway,
            CAST(RELATION_TO_JUNC AS STRING) AS Relation_Junction,
            CAST(RELATION_TO_TRAFFICWAY AS STRING) AS Relation_Trafficway,
            CAST(TYPE_INTERSECTION AS STRING) AS Type_of_Intersection,
            CAST(MANNER_OF_COLLISION AS STRING) AS Manner_of_Collision,
            CAST(SCH_BUS AS BOOLEAN) AS Sch_Bus_Involved,
            CAST(LATITUDE AS DECIMAL(7,5)) AS Latitude,
            CAST(LONGITUD AS DECIMAL(7,4)) AS Longitude
    FROM all_accidents
""")

"""FOR THE PERSONS FILE"""

#Filter out header columns
df_person = df_person.filter(df_person.AGE != 'AGE')

#Casting Year as Date for Comparisons
df_person = df_person.withColumn('DATE', df_person.YEAR.cast(types.DateType()))

#Car Model Year
df_person = df_person.withColumn('MODEL_YEAR', F.when((F.length('MOD_YEAR') < 4) & (F.col('MOD_YEAR') != '99'), F.concat(F.lit('19'), F.col('MOD_YEAR'))) \
    .when((F.col('MOD_YEAR') == '99') | (F.col('MOD_YEAR') == '9998') | (F.col('MOD_YEAR') == 'Unknown'), None).otherwise(df_person.MOD_YEAR))

#YEAR and UNIQ_ID
df_person = df_person.withColumn('SUB', F.substring('YEAR', 3, 2))
df_person = df_person.withColumn('UNIQ_ID', F.concat(F.col('ST_CASE'), F.col('SUB')))
                                
#Tracks Deaths as 1 and 0 alive Dead or Alive Column
df_person = df_person.withColumn('D_or_A', F.when(df_person.INJ_SEV == '4', 1).otherwise(0))

#AGE
df_person = df_person.withColumn("PERSON_AGE", F.when((F.col('AGE') == '99') & (F.year(df_person.AGE) <= 2008), None) \
    .when(F.col('AGE') == '0', 1).when((F.col('AGE') == '998') | (F.col('AGE') == '999'), None).otherwise(df_person.AGE))

#Gender
df_person = df_person.withColumn('GENDER', F.when(df_person.SEX == '1', 'Male').when(df_person.SEX == '2', 'Female') \
                                .when(df_person.SEX == '8', 'Not Reported').when(df_person.SEX == '9', 'Unknown') \
                                .otherwise(df_person.SEX))

#Race 
df_person = df_person.withColumn('PER_RACE', F.when(df_person.RACE == '0', 'Not a Fatality (Not Applicable)') \
                                .when(df_person.RACE == '1', 'White') \
                                .when(df_person.RACE == '2', 'Black') \
                                .when(df_person.RACE == '3', 'American Indian (Includes Alaska Native') \
                                .when(df_person.RACE == '4', 'Chinese') \
                                .when(df_person.RACE == '5', 'Japanese') \
                                .when(df_person.RACE == '6', 'Hawaiian (Includes Part-Hawaiian)') \
                                .when(df_person.RACE == '7', 'Filipino') \
                                .when(df_person.RACE == '18', 'Asian Indian') \
                                .when(df_person.RACE == '19', 'Other Indian (Includes South and Central America)') \
                                .when(df_person.RACE == '28', 'Korean') \
                                .when(df_person.RACE == '38', 'Samoan') \
                                .when(df_person.RACE == '48', 'Vietnamese') \
                                .when(df_person.RACE == '58', 'Guamanian') \
                                .when(df_person.RACE == '68', 'Other Asian or Pacific Islander') \
                                .when(df_person.RACE == '78', 'Combined Asian Pacific Islander (No Specific Race)') \
                                .when(df_person.RACE == '97', 'Multiple Races (Mixed)') \
                                .when(df_person.RACE == '98', 'All Other Races') \
                                .when(df_person.RACE == '99', 'Unknown') \
                                .otherwise(df_person.RACE))

#VEH_MAKE
df_person = df_person.withColumn('VEH_MAKE', F.when(df_person.MAKE == '1', 'American Motors').when(df_person.MAKE == '2', 'Jeep') \
                                .when(df_person.MAKE == '3', 'AM General').when(df_person.MAKE == '6', 'Chrysler').when(df_person.MAKE == '7', 'Dodge') \
                                 .when(df_person.MAKE == '8', 'Imperial').when(df_person.MAKE == '9', 'Plymouth').when(df_person.MAKE == '10', 'Eagle') \
                                 .when(df_person.MAKE == '12', 'Ford').when(df_person.MAKE == '13', 'Lincoln').when(df_person.MAKE == '14', 'Mercury') \
                                 .when(df_person.MAKE == '18', 'Buick').when(df_person.MAKE == '19', 'Cadillac').when(df_person.MAKE == '20', 'Chevrolet') \
                                 .when(df_person.MAKE == '21', 'Oldsmobile').when(df_person.MAKE == '22', 'Pontiac').when(df_person.MAKE == '23', 'GMC') \
                                 .when(df_person.MAKE == '24', 'Saturn').when(df_person.MAKE == '25', 'Grumman').when(df_person.MAKE == '26', 'Coda') \
                                 .when(df_person.MAKE == '29', 'Other Domestic').when(df_person.MAKE == '30', 'Volkswagen').when(df_person.MAKE == '31', 'Alfa Romeo') \
                                 .when(df_person.MAKE == '32', 'Audi').when(df_person.MAKE == '33', 'Austin/Austin-Healey').when(df_person.MAKE == '34', 'BMW') \
                                 .when((df_person.MAKE == '35') & (F.year(df_person.DATE) <= 1990), 'Datsun') \
                                 .when((df_person.MAKE == '35') & (F.year(df_person.DATE) >= 1991), 'Datsun/Nissan') \
                                 .when(df_person.MAKE == '36', 'Fiat').when(df_person.MAKE == '37', 'Honda').when(df_person.MAKE == '38', 'Isuzu') \
                                 .when(df_person.MAKE == '39', 'Jaguar').when(df_person.MAKE == '40', 'Lancia').when(df_person.MAKE == '41', 'Mazda') \
                                 .when(df_person.MAKE == '42', 'Mercedes-Benz').when(df_person.MAKE == '43', 'MG').when(df_person.MAKE == '44', 'Peugeot') \
                                 .when(df_person.MAKE == '45', 'Porsche').when(df_person.MAKE == '46', 'Renault').when(df_person.MAKE == '47', 'Saab') \
                                 .when(df_person.MAKE == '48', 'Subaru').when(df_person.MAKE == '49', 'Toyota').when(df_person.MAKE == '50', 'Triumph') \
                                 .when(df_person.MAKE == '51', 'Volvo').when(df_person.MAKE == '52', 'Mitsubishi').when(df_person.MAKE == '53', 'Suzuki') \
                                 .when(df_person.MAKE == '54', 'Acura').when(df_person.MAKE == '55', 'Hyundai').when(df_person.MAKE == '56', 'Merkur') \
                                 .when((df_person.MAKE == '57') & (F.year(df_person.DATE) <= 1990), 'Lexus') \
                                .when((df_person.MAKE == '57') & (F.year(df_person.DATE) >= 1991), 'Yugo') \
                                .when(df_person.MAKE == '58', 'Infinite') \
                                .when((df_person.MAKE == '59') & (F.year(df_person.DATE) <= 1990), 'Other Imports') \
                                .when((df_person.MAKE == '59') & (F.year(df_person.DATE) >= 1991), 'Lexus') \
                                .when((df_person.MAKE == '60') & (F.year(df_person.DATE) <= 1990), 'BSA') \
                                .when((df_person.MAKE == '60') & (F.year(df_person.DATE) >= 1991), 'Daihatsu') \
                                .when((df_person.MAKE == '61') & (F.year(df_person.DATE) <= 1990), 'Ducati') \
                                .when((df_person.MAKE == '61') & (F.year(df_person.DATE) >= 1991), 'Sterling') \
                                .when((df_person.MAKE == '62') & (F.year(df_person.DATE) <= 1990), 'Harley-Davidson') \
                                .when((df_person.MAKE == '62') & (F.year(df_person.DATE) >= 1991), 'Land Rover') \
                                .when((df_person.MAKE == '63') & (F.year(df_person.DATE) <= 1990), 'Kawasaki') \
                                .when((df_person.MAKE == '63') & (F.year(df_person.DATE) >= 1991), 'Kia') \
                                .when((df_person.MAKE == '64') & (F.year(df_person.DATE) <= 1990), 'Moto Guzzi') \
                                .when((df_person.MAKE == '64') & (F.year(df_person.DATE) >= 1991), 'Daewoo') \
                                .when((df_person.MAKE == '65') & (F.year(df_person.DATE) <= 1990), 'Norton') \
                                .when((df_person.MAKE == '65') & (F.year(df_person.DATE) >= 1991), 'Smart') \
                                .when(df_person.MAKE == '66', 'Mahindra') \
                                .when((df_person.MAKE == '67') & (F.year(df_person.DATE) <= 1990), 'Yamaha') \
                                .when((df_person.MAKE == '67') & (F.year(df_person.DATE) >= 1991), 'Scion') \
                                .when((df_person.MAKE == '69') & (F.year(df_person.DATE) <= 1990), 'Other Motor Cycle') \
                                .when((df_person.MAKE == '69') & (F.year(df_person.DATE) >= 1991), 'Other Imports') \
                                .when((df_person.MAKE == '70') & (F.year(df_person.DATE) <= 1990), 'Moped') \
                                .when((df_person.MAKE == '70') & (F.year(df_person.DATE) >= 1991), 'BSA') \
                                .when(df_person.MAKE == '71', 'Ducati').when(df_person.MAKE == '72', 'Harley-Davidson') \
                                 .when(df_person.MAKE == '73', 'Kawasaki').when(df_person.MAKE == '74', 'Moto Guzzi') \
                                 .when(df_person.MAKE == '75', 'Norton').when(df_person.MAKE == '76', 'Yamaha') \
                                 .when(df_person.MAKE == '77', 'Victory').when(df_person.MAKE == '78', 'Other Make Moped') \
                                 .when(df_person.MAKE == '79', 'Other Make Motored Cycle') \
                                 .when(df_person.MAKE == '80', 'Brockway').when(df_person.MAKE == '81', 'Diamond Reo/Reo') \
                                 .when(df_person.MAKE == '82', 'Freightliner').when(df_person.MAKE == '83', 'FWD') \
                                 .when(df_person.MAKE == '84', 'International Harvester').when(df_person.MAKE == '85', 'Kenworth') \
                                 .when(df_person.MAKE == '86', 'Mack').when(df_person.MAKE == '87', 'Peterbilt') \
                                 .when((df_person.MAKE == '88') & (F.year(df_person.DATE) <= 1990), 'White') \
                                 .when((df_person.MAKE == '88') & (F.year(df_person.DATE) >= 1991), 'Iveco/Magirus') \
                                 .when(df_person.MAKE == '89', 'White/Autocar, White/GMC').when(df_person.MAKE == '90', 'Bluebird') \
                                 .when(df_person.MAKE == '91', 'Eagle Coach').when(df_person.MAKE == '92', 'Gillig') \
                                 .when(df_person.MAKE == '93', 'MCI').when(df_person.MAKE == '94', 'Thomas Built') \
                                 .when(df_person.MAKE == '95', 'Other Truck/Bus').when(df_person.MAKE == '97', 'Not Reported') \
                                 .when(df_person.MAKE == '98', 'Other Make').when(df_person.MAKE == '99', 'Unknown Make') \
                                 .otherwise(df_person.MAKE)
                                )

###TEMP VIEW FOR PERSON FILES
df_person.createOrReplaceTempView('all_persons')

"""FOR VEHICLE FILE"""

df_vehicle = df_vehicle.filter(df_vehicle.ST_CASE != 'ST_CASE')

#YEAR and UNIQ_ID
df_vehicle = df_vehicle.withColumn('DATE', df_vehicle.YEAR.cast(types.DateType()))
df_vehicle = df_vehicle.withColumn('SUB', F.substring('YEAR', 3, 2))
df_vehicle = df_vehicle.withColumn('UNIQ_ID', F.concat(F.col('ST_CASE'), (F.col('SUB'))))

#License state Column
df_vehicle = df_vehicle.withColumn('L_STATE', states_udf(df_vehicle.L_STAT))

#Vehicle TRAV_SPEED
df_vehicle = df_vehicle.withColumn('TRAV_SPEED', F.when((df_vehicle.TRAV_SP == '97') & (F.year(df_vehicle.DATE) <= 2008), '> 96 mph') \
                                 .when((df_vehicle.TRAV_SP == '98') & (F.year(df_vehicle.DATE) <= 2008), 'Not Reported')\
                                 .when((df_vehicle.TRAV_SP == '99') & (F.year(df_vehicle.DATE) <= 2008), 'Unknown')\
                                 .when(df_vehicle.TRAV_SP == '997',  '> 151 mph')\
                                 .when(df_vehicle.TRAV_SP == '998', 'Not Reported')\
                                 .when(df_vehicle.TRAV_SP == '999', 'Unknown')\
                                 .otherwise(df_vehicle.TRAV_SP))
                                
#MODEL_YEAR
df_vehicle = df_vehicle.withColumn('MODEL_YEAR', F.when((F.length('MOD_YEAR') < 4) & (F.col('MOD_YEAR') != '99'), F.concat(F.lit('19'), F.col('MOD_YEAR'))) \
    .when((F.col('MOD_YEAR') == '99') | (F.col('MOD_YEAR') == '9998') | (F.col('MOD_YEAR') == 'Unknown'), None).otherwise(df_vehicle.MOD_YEAR))

#VEH_MAKE
df_vehicle = df_vehicle.withColumn('VEH_MAKE', F.when(df_vehicle.MAKE == '1', 'American Motors').when(df_vehicle.MAKE == '2', 'Jeep') \
                                .when(df_vehicle.MAKE == '3', 'AM General').when(df_vehicle.MAKE == '6', 'Chrysler').when(df_vehicle.MAKE == '7', 'Dodge') \
                                 .when(df_vehicle.MAKE == '8', 'Imperial').when(df_vehicle.MAKE == '9', 'Plymouth').when(df_vehicle.MAKE == '10', 'Eagle') \
                                 .when(df_vehicle.MAKE == '12', 'Ford').when(df_vehicle.MAKE == '13', 'Lincoln').when(df_vehicle.MAKE == '14', 'Mercury') \
                                 .when(df_vehicle.MAKE == '18', 'Buick').when(df_vehicle.MAKE == '19', 'Cadillac').when(df_vehicle.MAKE == '20', 'Chevrolet') \
                                 .when(df_vehicle.MAKE == '21', 'Oldsmobile').when(df_vehicle.MAKE == '22', 'Pontiac').when(df_vehicle.MAKE == '23', 'GMC') \
                                 .when(df_vehicle.MAKE == '24', 'Saturn').when(df_vehicle.MAKE == '25', 'Grumman').when(df_vehicle.MAKE == '26', 'Coda') \
                                 .when(df_vehicle.MAKE == '29', 'Other Domestic').when(df_vehicle.MAKE == '30', 'Volkswagen').when(df_vehicle.MAKE == '31', 'Alfa Romeo') \
                                 .when(df_vehicle.MAKE == '32', 'Audi').when(df_vehicle.MAKE == '33', 'Austin/Austin-Healey').when(df_vehicle.MAKE == '34', 'BMW') \
                                 .when((df_vehicle.MAKE == '35') & (F.year(df_vehicle.DATE) <= 1990), 'Datsun') \
                                 .when((df_vehicle.MAKE == '35') & (F.year(df_vehicle.DATE) >= 1991), 'Datsun/Nissan') \
                                 .when(df_vehicle.MAKE == '36', 'Fiat').when(df_vehicle.MAKE == '37', 'Honda').when(df_vehicle.MAKE == '38', 'Isuzu') \
                                 .when(df_vehicle.MAKE == '39', 'Jaguar').when(df_vehicle.MAKE == '40', 'Lancia').when(df_vehicle.MAKE == '41', 'Mazda') \
                                 .when(df_vehicle.MAKE == '42', 'Mercedes-Benz').when(df_vehicle.MAKE == '43', 'MG').when(df_vehicle.MAKE == '44', 'Peugeot') \
                                 .when(df_vehicle.MAKE == '45', 'Porsche').when(df_vehicle.MAKE == '46', 'Renault').when(df_vehicle.MAKE == '47', 'Saab') \
                                 .when(df_vehicle.MAKE == '48', 'Subaru').when(df_vehicle.MAKE == '49', 'Toyota').when(df_vehicle.MAKE == '50', 'Triumph') \
                                 .when(df_vehicle.MAKE == '51', 'Volvo').when(df_vehicle.MAKE == '52', 'Mitsubishi').when(df_vehicle.MAKE == '53', 'Suzuki') \
                                 .when(df_vehicle.MAKE == '54', 'Acura').when(df_vehicle.MAKE == '55', 'Hyundai').when(df_vehicle.MAKE == '56', 'Merkur') \
                                 .when((df_vehicle.MAKE == '57') & (F.year(df_vehicle.DATE) <= 1990), 'Lexus') \
                                .when((df_vehicle.MAKE == '57') & (F.year(df_vehicle.DATE) >= 1991), 'Yugo') \
                                .when(df_vehicle.MAKE == '58', 'Infinite') \
                                .when((df_vehicle.MAKE == '59') & (F.year(df_vehicle.DATE) <= 1990), 'Other Imports') \
                                .when((df_vehicle.MAKE == '59') & (F.year(df_vehicle.DATE) >= 1991), 'Lexus') \
                                .when((df_vehicle.MAKE == '60') & (F.year(df_vehicle.DATE) <= 1990), 'BSA') \
                                .when((df_vehicle.MAKE == '60') & (F.year(df_vehicle.DATE) >= 1991), 'Daihatsu') \
                                .when((df_vehicle.MAKE == '61') & (F.year(df_vehicle.DATE) <= 1990), 'Ducati') \
                                .when((df_vehicle.MAKE == '61') & (F.year(df_vehicle.DATE) >= 1991), 'Sterling') \
                                .when((df_vehicle.MAKE == '62') & (F.year(df_vehicle.DATE) <= 1990), 'Harley-Davidson') \
                                .when((df_vehicle.MAKE == '62') & (F.year(df_vehicle.DATE) >= 1991), 'Land Rover') \
                                .when((df_vehicle.MAKE == '63') & (F.year(df_vehicle.DATE) <= 1990), 'Kawasaki') \
                                .when((df_vehicle.MAKE == '63') & (F.year(df_vehicle.DATE) >= 1991), 'Kia') \
                                .when((df_vehicle.MAKE == '64') & (F.year(df_vehicle.DATE) <= 1990), 'Moto Guzzi') \
                                .when((df_vehicle.MAKE == '64') & (F.year(df_vehicle.DATE) >= 1991), 'Daewoo') \
                                .when((df_vehicle.MAKE == '65') & (F.year(df_vehicle.DATE) <= 1990), 'Norton') \
                                .when((df_vehicle.MAKE == '65') & (F.year(df_vehicle.DATE) >= 1991), 'Smart') \
                                .when(df_vehicle.MAKE == '66', 'Mahindra') \
                                .when((df_vehicle.MAKE == '67') & (F.year(df_vehicle.DATE) <= 1990), 'Yamaha') \
                                .when((df_vehicle.MAKE == '67') & (F.year(df_vehicle.DATE) >= 1991), 'Scion') \
                                .when((df_vehicle.MAKE == '69') & (F.year(df_vehicle.DATE) <= 1990), 'Other Motor Cycle') \
                                .when((df_vehicle.MAKE == '69') & (F.year(df_vehicle.DATE) >= 1991), 'Other Imports') \
                                .when((df_vehicle.MAKE == '70') & (F.year(df_vehicle.DATE) <= 1990), 'Moped') \
                                .when((df_vehicle.MAKE == '70') & (F.year(df_vehicle.DATE) >= 1991), 'BSA') \
                                .when(df_vehicle.MAKE == '71', 'Ducati').when(df_vehicle.MAKE == '72', 'Harley-Davidson') \
                                 .when(df_vehicle.MAKE == '73', 'Kawasaki').when(df_vehicle.MAKE == '74', 'Moto Guzzi') \
                                 .when(df_vehicle.MAKE == '75', 'Norton').when(df_vehicle.MAKE == '76', 'Yamaha') \
                                 .when(df_vehicle.MAKE == '77', 'Victory').when(df_vehicle.MAKE == '78', 'Other Make Moped') \
                                 .when(df_vehicle.MAKE == '79', 'Other Make Motored Cycle') \
                                 .when(df_vehicle.MAKE == '80', 'Brockway').when(df_vehicle.MAKE == '81', 'Diamond Reo/Reo') \
                                 .when(df_vehicle.MAKE == '82', 'Freightliner').when(df_vehicle.MAKE == '83', 'FWD') \
                                 .when(df_vehicle.MAKE == '84', 'International Harvester').when(df_vehicle.MAKE == '85', 'Kenworth') \
                                 .when(df_vehicle.MAKE == '86', 'Mack').when(df_vehicle.MAKE == '87', 'Peterbilt') \
                                 .when((df_vehicle.MAKE == '88') & (F.year(df_vehicle.DATE) <= 1990), 'White') \
                                 .when((df_vehicle.MAKE == '88') & (F.year(df_vehicle.DATE) >= 1991), 'Iveco/Magirus') \
                                 .when(df_vehicle.MAKE == '89', 'White/Autocar, White/GMC').when(df_vehicle.MAKE == '90', 'Bluebird') \
                                 .when(df_vehicle.MAKE == '91', 'Eagle Coach').when(df_vehicle.MAKE == '92', 'Gillig') \
                                 .when(df_vehicle.MAKE == '93', 'MCI').when(df_vehicle.MAKE == '94', 'Thomas Built') \
                                 .when(df_vehicle.MAKE == '95', 'Other Truck/Bus').when(df_vehicle.MAKE == '97', 'Not Reported') \
                                 .when(df_vehicle.MAKE == '98', 'Other Make').when(df_vehicle.MAKE == '99', 'Unknown Make') \
                                 .otherwise(df_vehicle.MAKE)
                                )

#HEIGT AND WEIGHT
df_vehicle = df_vehicle.withColumn('HEIGHT', F.when((F.col('DR_HGT') == '998') | ((F.col('DR_HGT') == '999')), None).otherwise(df_vehicle.DR_HGT))
df_vehicle = df_vehicle.withColumn('WEIGHT', F.when((F.col('DR_WGT') == '997') | ((F.col('DR_HGT') == '998')) | ((F.col('DR_HGT') == '999')), None).otherwise(df_vehicle.DR_WGT))

###TEMP VIEW FOR VEHICLE FILES
df_vehicle.createOrReplaceTempView('all_vehicles')

df_person_vehicle_joined = spark.sql("""
SELECT 
    CAST(per.UNIQ_ID AS STRING) AS Case_Number,
    YEAR(per.YEAR) AS Year,
    CAST(per.PER_NO AS STRING) AS Person_Num,
    CAST(per.PERSON_AGE AS INTEGER) AS Age,
    CAST(per.GENDER AS STRING) AS Sex,
    CAST(per.PER_RACE AS STRING) AS Race,
    CAST(per.D_or_A AS INTEGER) AS Fatal,
    CAST(per.VEH_MAKE AS STRING) AS Make,
    CAST(per.MODEL_YEAR AS STRING) AS Model_Year,
    CAST(per.VEH_NO AS STRING) AS Veh_Num,
    CAST(veh.TRAV_SPEED AS STRING) AS Veh_Speed

FROM
    all_persons AS per
LEFT JOIN
    all_vehicles AS veh
ON
    per.UNIQ_ID = veh.UNIQ_ID
    AND
    per.VEH_NO = veh.VEH_NO
""")

#Writing the query

df_accident_vehicle_joined = spark.sql("""
    SELECT
        CAST(acc.UNIQ_ID AS STRING) AS Case_Number,
        YEAR(acc.YEAR) AS Year,
        CAST(acc.DATE AS DATE) AS Date,
        CAST(acc.DAY_OF_WEEK AS STRING) AS Day_Of_Week,
        CAST(acc.STATE AS STRING) AS State,
        CAST(acc.CITY AS STRING) AS City,
        CAST(acc.COUNTY AS STRING) AS County,
        CAST(veh.VEH_NO AS STRING) AS Veh_Num,
        CAST(veh.VEH_MAKE AS STRING) AS Make,
        CAST(veh.MODEL_YEAR AS STRING) AS Model_Year,
        CAST(veh.TRAV_SPEED AS STRING) AS Veh_Speed,
        CAST(veh.HEIGHT AS INTEGER) AS Driver_Height,
        CAST(veh.WEIGHT AS INTEGER) AS Driver_Weight,
        CAST(veh.L_STATE AS STRING) AS License_State,
        CAST(veh.DEATHS AS INTEGER) AS Deaths_in_Veh,
        CAST(acc.TIME_OF_ACCIDENT AS STRING) AS Time_of_Accident,
        CAST(acc.NOTIFICATION_TIME AS STRING) AS Notification_Time,
        CAST(acc.EMS_ARRIVAL_TIME AS STRING) AS EMS_Arrival_Time,
        CAST(acc.HOSP_ARRIVAL_TIME AS STRING) AS Hospital_Arrival_Time,
        CAST(acc.LIGHT_CONDITION AS STRING) AS Light_Conditions,
        CAST(acc.WEATHER_CONDITION AS STRING) AS Weather_Conditions,
        CAST(acc.RURAL_OR_URBAN AS STRING) AS Rural_Or_Urban,
        CAST(acc.ROUTE_TRAFFICWAY AS STRING) AS Route_Trafficway,
        CAST(acc.RELATION_TO_JUNC AS STRING) AS Relation_Junction,
        CAST(acc.RELATION_TO_TRAFFICWAY AS STRING) AS Relation_Trafficway,
        CAST(acc.TYPE_INTERSECTION AS STRING) AS Type_of_Intersection,
        CAST(acc.MANNER_OF_COLLISION AS STRING) AS Manner_of_Collision,
        CAST(acc.LATITUDE AS DECIMAL(7,5)) AS Latitude,
        CAST(acc.LONGITUD AS DECIMAL(7,4)) AS Longitude

    FROM
        all_accidents AS acc
    LEFT JOIN
        all_vehicles AS veh
    ON
        acc.UNIQ_ID = veh.UNIQ_ID
""")

#Query for ACCIDENT FILE JOIN PERSON FILE

df_accident_person_joined = spark.sql("""
     SELECT
        CAST(acc.UNIQ_ID AS STRING) AS Case_Number,
        YEAR(acc.YEAR) AS Year,
        CAST(acc.DATE AS DATE) AS Date,
        CAST(acc.DAY_OF_WEEK AS STRING) AS Day_Of_Week,
        CAST(acc.STATE AS STRING) AS State,
        CAST(acc.CITY AS STRING) AS City,
        CAST(per.PER_NO AS STRING) AS Person_Num,
        CAST(per.PERSON_AGE AS INTEGER) AS Age,
        CAST(per.GENDER AS STRING) AS Sex,
        CAST(per.PER_RACE AS STRING) AS Race,
        CAST(per.D_or_A AS INTEGER) AS Fatal,
        CAST(per.VEH_MAKE AS STRING) AS Make,
        CAST(per.MODEL_YEAR AS STRING) AS Model_Year,
        CAST(per.VEH_NO AS STRING) AS Veh_Num,
        CAST(acc.TIME_OF_ACCIDENT AS STRING) AS Time_of_Accident,
        CAST(acc.NOTIFICATION_TIME AS STRING) AS Notification_Time,
        CAST(acc.EMS_ARRIVAL_TIME AS STRING) AS EMS_Arrival_Time,
        CAST(acc.HOSP_ARRIVAL_TIME AS STRING) AS Hospital_Arrival_Time,
        CAST(acc.LIGHT_CONDITION AS STRING) AS Light_Conditions,
        CAST(acc.WEATHER_CONDITION AS STRING) AS Weather_Conditions,
        CAST(acc.NHS AS BOOLEAN) AS On_Nat_Highway_Sys,
        CAST(acc.RURAL_OR_URBAN AS STRING) AS Rural_Or_Urban,
        CAST(acc.ROUTE_TRAFFICWAY AS STRING) AS Route_Trafficway,
        CAST(acc.RELATION_TO_JUNC AS STRING) AS Relation_Junction,
        CAST(acc.RELATION_TO_TRAFFICWAY AS STRING) AS Relation_Trafficway,
        CAST(acc.TYPE_INTERSECTION AS STRING) AS Type_of_Intersection,
        CAST(acc.MANNER_OF_COLLISION AS STRING) AS Manner_of_Collision,
        CAST(acc.LATITUDE AS DECIMAL(7,5)) AS Latitude,
        CAST(acc.LONGITUD AS DECIMAL(7,4)) AS Longitude

        FROM
            all_accidents AS acc
        LEFT JOIN
            all_persons AS per
        ON
            acc.UNIQ_ID = per.UNIQ_ID
""")

df_result.write.format('bigquery') \
    .option('project', '<YOUR GOOGLE CLOUD PROVIDER PROJECT>') \
    .option('dataset', '<YOUR DATASET NAME>') \
    .option('table', '<TABLE NAME TO SAVE AS>') \
    .save()

df_accident_person_joined.write.format('bigquery') \
    .option('project', '<YOUR GOOGLE CLOUD PROVIDER PROJECT>') \
    .option('dataset', '<YOUR DATASET NAME>') \
    .option('table', '<TABLE NAME TO SAVE AS>') \
    .save()


df_person_vehicle_joined.write.format('bigquery') \
    .option('project', '<YOUR GOOGLE CLOUD PROVIDER PROJECT>') \
    .option('dataset', '<YOUR DATASET NAME>') \
    .option('table', '<TABLE NAME TO SAVE AS>') \
    .save()


df_accident_vehicle_joined.write.format('bigquery') \
    .option('project', '<YOUR GOOGLE CLOUD PROVIDER PROJECT>') \
    .option('dataset', '<YOUR DATASET NAME>') \
    .option('table', '<TABLE NAME TO SAVE AS>') \
    .save()