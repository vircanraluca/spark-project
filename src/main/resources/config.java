log4j.rootCategory=INFO, console, file

# Configurare Console Appender
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n

# Configurare File Appender
log4j.appender.file=org.apache.log4j.RollingFileAppender
log4j.appender.file.File=logs/spark-data-engineering.log
log4j.appender.file.MaxFileSize=10MB
log4j.appender.file.MaxBackupIndex=10
log4j.appender.file.layout=org.apache.log4j.PatternLayout
log4j.appender.file.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n

# Setări pentru Spark
log4j.logger.org.apache.spark=WARN
log4j.logger.org.spark_project.jetty=WARN
log4j.logger.org.apache.spark.scheduler.TaskSetManager=WARN
log4j.logger.org.apache.spark.scheduler.DAGScheduler=INFO
log4j.logger.org.apache.spark.storage.BlockManagerInfo=WARN

# Setări pentru aplicația noastră
log4j.logger.com.dataengineering.spark=INFO

# Reducere zgomot pentru anumite componente
log4j.logger.org.apache.hadoop=WARN
log4j.logger.org.apache.parquet=ERROR
log4j.logger.parquet=ERROR

# Dezactivare logging pentru Jetty
log4j.logger.org.spark-project.jetty=WARN
log4j.logger.org.spark-project.jetty.util.component.AbstractLifeCycle=ERROR
log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper=WARN
log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter=WARN

# Configurare pentru monitorizare performanță
log4j.logger.com.dataengineering.spark.PerformanceMonitor=DEBUG