FROM apache/spark

# copy the jar into the container
COPY /target/lab3-mastodon-1.0-SNAPSHOT.jar job.jar

# copy the properties folder into container
COPY log4j.properties /opt/spark/work-dir/log4j.properties

# copy the appconfig folder into container
COPY app.conf /opt/spark/work-dir/app.conf

# submit (run) the spark job
CMD ["/opt/spark/bin/spark-submit", "--conf", "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:///opt/spark/work-dir/log4j.properties", "--class", "edu.upf.MastodonStreamingExample", "job.jar"]
