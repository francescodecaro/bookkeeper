mvn clean verify sonar:sonar -Dcoverage \
 -Dsonar.projectKey=bookkeeper \
 -Dsonar.host.url=http://localhost:9000 \
 -Dsonar.login=151f13ffb32b3023ac6b6141006a3f97d2ea120a


 mvn sonar:sonar -Dsonar.projectKey=bookkeeper \
  -Dsonar.host.url=http://localhost:9000 \
  -Dsonar.login=151f13ffb32b3023ac6b6141006a3f97d2ea120a \
  -Dsonar.coverage.jacoco.xmlReportPaths=bookkeeper-server/target/site/jacoco/jacoco.xml

   mvn sonar:sonar -Dsonar.projectKey=bookkeeper \
    -Dsonar.host.url=http://localhost:9000 \
    -Dsonar.login=151f13ffb32b3023ac6b6141006a3f97d2ea120a \
    -Dsonar.coverage.jacoco.xmlReportPaths=tests/target/site/jacoco-aggregate/jacoco.xml


mkdir -p bookkeeper-server/target/site/jacoco
java -jar /Users/francesco/Documents/UNIVERSITA/ISW2/isw2-jcs/lib/jacococli.jar report bookkeeper-server/target/jacoco.exec --classfiles bookkeeper-server/target/bookkeeper-server-4.15.0-SNAPSHOT.jar --sourcefiles bookkeeper-server/src --html bookkeeper-server/target/site/jacoco/jcs-coverage --xml bookkeeper-server/target/site/jacoco/jacoco.xml
