# rodar spark-submit --jars /home/nivas/postgresql-42.2.23.jar spark_json.py

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.streaming import *

if __name__=="__main__":
    spark = SparkSession.builder.appName("Streaming CSV").getOrCreate()
    print('##########################################################')
    print('###### Criando Spark Session e App Streaming CSV....######')
    print('##########################################################')

    userSchema = StructType().add("id", "string").add("tipo", "string").add("nome", "string").add("ppu", "string")
    df = spark.readStream.option("sep", ",").schema(userSchema).csv("/home/nivas/testestream")

    diretorio = "/home/nivas/temp"

    def atualizabanco(dataf, batchId):
        dataf.write.format("jdbc")\
            .option("url","jdbc:postgresql://localhost:5438/banco")\
            .option("dbtable","streaming.usuarios")\
            .option("user","*********************")\
            .option("password","*****************")\
            .option("driver","org.postgresql.Driver")\
            .mode("append")\
            .save()
        print('################################################')
        print('##### Dados atualizados ....####################')
        print('################################################')

    Stcal = df.writeStream.foreachBatch(atualizabanco).trigger(processingTime="5 second").option("checkpointlocation",diretorio).start()
    Stcal.awaitTermination()



