mvn clean verify sonar:sonar -D coverage \
  -Dsonar.projectKey=bookkeeper \
  -Dsonar.host.url=http://localhost:9000 \
  -Dsonar.login=151f13ffb32b3023ac6b6141006a3f97d2ea120a