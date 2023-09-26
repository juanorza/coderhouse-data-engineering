## ----------------------------------------------------------------------
## Welcome to the Coderhouse's Data Engineering example project
## ----------------------------------------------------------------------


build: ## build the solution
	echo "Building Airflow locally using the LocalExecutor"
	docker-compose -f docker-compose.yml build --progress=plain --no-cache

run: ## run the solution
	echo "Running Airflow locally using the LocalExecutor"
	docker-compose -f docker-compose.yml up -d

stop: ## stop running every container
	echo "Stopping all containers"
	docker-compose -f docker-compose.yml down -v --remove-orphans

clean: ## stop and delete containers, delete volumes with database data and download images
	echo "Cleaning containers and volumes"
	docker compose down --volumes --rmi all

get-admin-password: ## get the admin's password
	docker exec -it de-3erentregable-webserver-1 cat standalone_admin_password.txt

bash: ## enter the airflow container with bash
	docker exec -it de-3erentregable-webserver-1 bash