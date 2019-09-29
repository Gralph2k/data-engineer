create-network:
	docker network create election-network

run:
	mvn clean package -DskipTests
	docker-compose up --build

clean:
	cp producer/src/test/resources/Template/* Data/Source
	rm Data/ErrorRows/*
	rm Data/Processed/*
	rm Data/SuccessRows/*
