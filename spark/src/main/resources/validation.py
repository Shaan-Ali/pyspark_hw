import com.github.mrpowers.spark.daria.sql.DataFrameValidator

inputDF = ( spark .readStream .schema(jsonSchema)
    .option("maxFilesPerTrigger", 100) #slow it down for tutorial
    .option("badRecordsPath", bad_records_path) #any bad records will go here
    .json(sensor_path) #the source
    .withColumn("INPUT_FILE_NAME", input_file_name()) #maintain file path
    .withColumn("PROCESSED_TIME", current_timestamp()) #add a processing timestamp at the time of processing
    .withWatermark("PROCESSED_TNE", "1 minute") #optional: window for out of order data
)

def withFullName()(df: DataFrame): DataFrame = {
  validatePresenceOfColumns(df, Seq("first_name", "last_name"))
  df.withColumn(
    "full_name",
    concat_ws(" ", col("first_name"), col("last_name"))
  )
}

def withIsSeniorCitizen()(df: DataFrame): DataFrame = {
  df.withColumn("is_senior_citizen", df("age") >= 65)
}