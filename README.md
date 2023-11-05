## Data engirneering project
This project is about creating an investment platform, based on the the [following tutorial](https://towardsdev.com/data-engineering-project-creating-an-investing-platform-part-1-e777b5bd27cd).
The tools used are the following : 

1. Python as programming laguage ;
2. Airflow as pipeline orchestrator ;
3. Flask as webdev framework ;
4. Docker (mainly for running Airflow and local databases) ;
5. GCP (Storage and BigQuery).

Some changes were made, compared to the tutorial :
- I ran Airlfow following the [official documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) (instead of using the yaml file provided in the tutorial);
- Instead date from [Fixer.io](https://fixer.io/), I used [FrankFurter.app](https://www.frankfurter.app/), which provides a free API for the same data, and doesn't require an API Key ;
- I rewrote some parts of the dag in order for it to suits my needs, but it still follows the guideline in the tutorial ;

## General architecture :
First we build a first ETL to extract some historical data from the API and load it to the cloud Storage (Google) and BigQuery ; then we build a scheduled ETL to extract the data once a day and insert it into BigQuery, and finaly we create a platform to exploit the data, using Flask. Which gives the following overview :

![Architecture overview](https://github.com/drux31/Investing_platform/blob/main/general_arch.png)

The code from the tutorial could be greatly simplified, but I did not bother in order to easily follow the steps.
The pipeline consists of two dags : 
- one that first loads a bunch of data to BigQuery ;
- a second one that loads daily data into BigQuery.
The main difference between the two is that the end_date for data extraction from the API is parameterized.
For the second dag, if the end date has no data, the API wil return the data from the prvious date. So I create a code that force to get the following date instead.