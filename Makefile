app      := flights
stage    := prod
database := /app/data/db/$(stage).duckdb
env_file := --env-file=.env
volumes  := -v ./data:/app/data -v ./dbt:/app/dbt
dockerun := docker run  $(env_file) $(volumes)
dbtpath  := --project-dir dbt --profiles-dir dbt
uvrun    := $(dockerun) $(app) uv run


help:           	## Show this help.
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'
 
build:			## Build the docker
	docker build -t $(app) .

update:			## Updates historical data up to the current day 
	$(uvrun) python cli.py update BOG

raw-data:      		## Reads, cleans and transforms the data and creates the raw data
	$(uvrun) python cli.py process

remove_database: 	## Remove existing database 
	rm -rf $(database)

dbt-seed:  		## Upload raw.csv file to bronze schema
	$(uvrun) dbt seed $(dbtpath)

dbt-run: 		## Create silver and gold schemas
	$(uvrun) dbt run $(dbtpath)


dbt-test: 		## Run unnit tests
	$(uvrun) dbt test $(dbtpath)

database: build update raw-data remove_database dbt-seed dbt-run dbt-test  ## -- Run al stemps to create database
	echo 'Database created'

docs: 			## Create docs generated by dbt to uderstand the lineage
	$(uvrun) dbt docs genearte $(dbtpath) && uv run dbt docs serve $(dbtpath)

sql: 			## Create a sql-editor in terminal
	$(dockerun) -it $(app) uv run harlequin -r $(database)

orchestrator:  		## Create a the dagster orchestrator to ingest real-time data
	$(dockerun) -p 3000:3000 $(app) uv run dagit -h 0.0.0.0 -p 3000 -f real_time_planes.py 

