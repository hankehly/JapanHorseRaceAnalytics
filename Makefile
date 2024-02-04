mlflow_server:
	mlflow server \
		--host 127.0.0.1 \
		--port 8080 \
		--backend-store-uri postgresql://admin:admin@127.0.0.1:5432/mlflow \
		--default-artifact-root ./mlruns

start_spark_server:
	spark-submit \
		--master 'local[*]' \
		--conf 'spark.executor.extraJavaOptions=-Duser.timezone=Etc/UTC' \
		--conf 'spark.eventLog.enabled=false' \
		--conf 'spark.sql.warehouse.dir=/Users/hankehly/Projects/JapanHorseRaceAnalytics/spark-warehouse' \
		--conf "spark.executor.extraClassPath=/Users/hankehly/Projects/JapanHorseRaceAnalytics/jars/postgresql-42.7.1.jar" \
		--conf "spark.driver.extraClassPath=/Users/hankehly/Projects/JapanHorseRaceAnalytics/jars/postgresql-42.7.1.jar" \
		--jars /Users/hankehly/Projects/JapanHorseRaceAnalytics/jars/postgresql-42.7.1.jar \
		--packages 'org.apache.spark:spark-sql_2.12:3.4.0,org.apache.spark:spark-hive_2.12:3.4.0' \
		--class 'org.apache.spark.sql.hive.thriftserver.HiveThriftServer2' \
		--name 'Thrift JDBC/ODBC Server'
