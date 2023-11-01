## Data engirneering project
This project is about creating an investment platform, based on the the [following tutorial](https://towardsdev.com/data-engineering-project-creating-an-investing-platform-part-1-e777b5bd27cd).
The tools used are the following : 

1. Python as programming laguage ;
2. Airflow as pipeline orchestrator ;
3. Flask as webdev framework ;
4. Docker (mainly for running Airflow and local databases) ;
5. GCP (Storage and BigQuery).

Some minor changes were made, compared to the tutorial :
- I ran Airlfow following the [official documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) (instead of using the yaml file provided in the tutorial);
- Instead date from [Fixer.io](https://fixer.io/), I used [FrankFurter.app](https://www.frankfurter.app/), which provides a free API for the same data, and doesn't require an API Key.

## General architecture :
First we build a first ETL to extract some historical data from the API and load it to the cloud Storage (Google) and BigQuery ; then we build a scheduled ETL to extract the data once a day and insert it into BigQuery, and finaly we create a platform to exploit the data, using Flask. Which gives the following overview :
![Architecture overview](general_architecture.png)

