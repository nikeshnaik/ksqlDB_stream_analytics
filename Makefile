kafka-whirl-up:
	docker-compose -f ./kafka_io/docker_compose.yml up -d --remove-orphans

kafka-wind-down:
	docker-compose -f ./kafka_io/docker_compose.yml down 

init-warehouse:
	docker-compose -f ./data_warehouse/olap-infra.yml up -d --remove-orphans

down-the-warehouse:
	docker-compose --file ./data_warehouse/olap-infra.yml down

setup-local-infra: kafka-whirl-up init-warehouse

tear-down-local-infra: kafka-wind-down down-the-warehouse