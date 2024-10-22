# airflow-docker-ICC

Pipeline for the copy data from API to Database (MySQL).

> # Set up mysql in Docker

* Install Docker in local machine
* Pull the Docker Image for MySQL
 ```
 docker pull mysql:latest 
 ```
 * Deploy and Start the MySQL Container
 ```
 docker run --name=[container_name] -d mysql:latest
 ```
 * Run mysql docker container
 ```
 docker exec -it [container_name] bash
 ````
 
> # Set up airflow in Docker

* Create the airflow folder and download Docker compose file in that folder. https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml
* Open the folder in VS code and open new terminal
* create the folder under the airflow folder using the below command
```
mkdir ./dags /.logs ./plugins
```
* create the environmental variables and put in the ```.env``` file
```
echo -e "AIRFLOW_UID=501 \n AIRFLOW_GID=0" > .env
```
* Installing the airflow
```
docker-compose up airflow-init
```
* Now run the airflow 
```
docker-compose up
```
* Open the http://localhost:8080 use the credentials ```username```:AIRFLOW ```possword```:AIRFLOW

# AIRFLOW DAG 
* Open the airflow folder in VS code and open the new terminal.
* Install the requirements useing the command
```
pip install -r requirements.txt
```
* Go to the ```dags``` directory
* create the new dag file
* copy the ```icc.py```  file and run it









 
 
