FROM openjdk:8u181-jdk
## ----
## Install Maven
#ARG MAVEN_VERSION=3.6.1
#ARG USER_HOME_DIR="/root"
#RUN mkdir -p /usr/share/maven && \
#curl -fsSL http://apache.osuosl.org/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz | tar -xzC /usr/share/maven --strip-components=1 && \
#ln -s /usr/share/maven/bin/mvn /usr/bin/mvn
#ENV MAVEN_HOME /usr/share/maven
#ENV MAVEN_CONFIG "$USER_HOME_DIR/.m2"
## speed up Maven JVM a bit
#ENV MAVEN_OPTS="-XX:+TieredCompilation -XX:TieredStopAtLevel=1"
## Install project dependencies and keep sources
## make source folder
#RUN mkdir -p /usr/src/app
#WORKDIR /usr/src/app
## install maven dependency packages (keep in image)
#RUN mvn clean compile assembly:single && rm -rf target
#COPY pom.xml /usr/src/app
#RUN mvn -T 1C install && rm -rf target
## copy other source files (keep in image)
#COPY src /usr/src/app/src
#COPY /usr/src/app/target/keyvaluestore-1.0-SNAPSHOT-jar-with-dependencies.jar /usr/src/app
#EXPOSE 9000
#CMD ["java -jar keyvaluestore-1.0-SNAPSHOT-jar-with-dependencies.jar file:///usr/src/app/tmp.txt"]
## ----

FROM maven:3.6.1-jdk-8 as build
WORKDIR /app
COPY --from=clone /app/keyvaluestore /app
RUN mvn clean compile assembly:single

FROM openjdk:8-jre-alpine
ARG artifactid
ARG version
ENV artifact ${artifactid}-${version}.jar
WORKDIR /app
COPY --from=build /app/target/${artifact} /app
EXPOSE 8080
CMD ["java -jar ${artifact}"]