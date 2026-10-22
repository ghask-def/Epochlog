# Build from the repository root:
#   docker build -f deploy/Dockerfile -t epochlog .
FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY . .
RUN ./gradlew :deploy:installDist --no-daemon -q

FROM eclipse-temurin:21-jre
COPY --from=build /src/deploy/build/install/epochlog /opt/epochlog
ENV PATH=/opt/epochlog/bin:$PATH
WORKDIR /var/lib/epochlog
EXPOSE 9090 9092
CMD ["epochlog-broker"]
