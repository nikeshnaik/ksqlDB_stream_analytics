kafka-whirl-up:
	docker-compose -f ./kafka_io/docker_compose.yml up -d --remove-orphans

kafka-wind-down:
	docker-compose -f ./kafka_io/docker_compose.yml down 

init-warehouse:
	python -m data_warehouse.init_dwh

setup-local-infra: kafka-whirl-up init-warehouse

tear-down-local-infra: kafka-wind-down 
