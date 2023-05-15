import pkg_resources
import subprocess
from pyspark.sql import SparkSession
import pyspark

print('PySpark:', pyspark.__version__, '\n')

spark = SparkSession.builder.getOrCreate()
print('Spark:', spark.version, '\n')

'''java_version = subprocess.check_output(
    ['java', '-version'], stderr=subprocess.STDOUT)
print('Java:', java_version.decode(), '\n')'''

'''installed_packages = pkg_resources.working_set
for package in installed_packages:
    print('Bibiliotecas:', package)'''
