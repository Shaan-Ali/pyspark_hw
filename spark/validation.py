# https://docs.databricks.com/spark/latest/spark-sql/handling-bad-records.html

inputDF = ( spark .readStream .schema(jsonSchema)
    .option("maxFilesPerTrigger", 100) #slow it down for tutorial
    .option("badRecordsPath", bad_records_path) #any bad records will go here
    .json(sensor_path) #the source
    .withColumn("INPUT_FILE_NAME", input_file_name()) #maintain file path
    .withColumn("PROCESSED_TIME", current_timestamp()) #add a processing timestamp at the time of processing
    .withWatermark("PROCESSED_TNE", "1 minute") #optional: window for out of order data
)