kafka-whirl-up:
	docker-compose -f ./kafka_io/docker_compose.yml up -d --remove-orphans

kafka-wind-down:
	docker-compose -f ./kafka_io/docker_compose.yml down 

setup-local-infra: kafka-whirl-up 

tear-down-local-infra: kafka-wind-down 