
from pyspark import HiveContext
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
import json

def analyzeFunction(t):
	accum_dict = t[1]
	accum_dict['ROWS_TOTAL'].add(1)
	rows = t[0]
	
	if rows[0] == 0 & rows[1] == 0:
		accum_dict['UNWATCHED_TOTAL'].add(1)
		return 'ignore'
	
	return rows	


class TestAccum():

    def test_method(self, input_data, output_path, analytical_path):
        
        conf = SparkConf() \
        .setAppName("Spark Job with RDDs and Accumulators")

		spark_context = SparkContext(conf = conf)
		sqlcontext = SQLContext(spark_context)
		
		a_dict = {
            'ROWS_TOTAL': sc.accumulator(0),
            'UNWATCHED_TOTAL': sc.accumulator(0)
        }
		
		# entertainment_data
		
		# id name series_watched add_watched
		# 1  ravi    1				1
		# 2	 surya	 0				1
		# 3  mamu    0				0
		# 4	 Tatu	 1				1
		# 5  Garu    1				1
		# 6	 Frank	 0				0
		# 7  Mahamad 1				1
		# 8	 Nano	 0				1
        rdd = spark_context.textFile(input_data) \
            .flatMap(lambda x: x.split("\n")) \
            .map(lambda s: list(csv.reader([s.encode('utf-8')]))[0]) \
            .map(lambda x: (x, a_dict)) \
            .map(analyzeFunction) \
            .filter(lambda x: 'ignore' not in x) \
            .coalesce(20)
        df = sqlcontext.createDataFrame(rdd, SCHEMA)
        df.write.parquet(output_file)
        json_file = json.dumps(accum_dict)
        json_file.write.json(analytical_path)

if __name__ == "__main__":
    test = TestAccum()
    test.test_method('/entertainment_data', '/result_entertainment_data', '/analytical_data')
