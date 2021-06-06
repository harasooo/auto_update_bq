test:
	docker-compose run --rm api bash -c "pytest --log-cli-level=INFO" --build