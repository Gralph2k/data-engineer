create-network:
	docker network create election-network

run:
	mvn clean package -DskipTests
	docker-compose up --build

clean:
	./clean.sh