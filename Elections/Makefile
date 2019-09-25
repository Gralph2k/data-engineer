create-network:
	docker network create election-network

run-appliance:
	mvn clean package -pl producer -DskipTests
	docker-compose up --build