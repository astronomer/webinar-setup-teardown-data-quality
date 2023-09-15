Overview
========

Hello! This project contains examples of how to data quality checks and setup/ teardown tasks in Airflow. 
It was used in the [Efficient data quality checks with Airflow 2.7](https://www.astronomer.io/events/webinars/efficient-data-quality-checks-with-airflow-2-7/) webinar.

Deploy Your Project Locally
===========================

1. Start Airflow on your local machine by running `astro dev start`.

This command will spin up 4 Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database, running on port `5432`
- Webserver: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- Triggerer: The Airflow component responsible for triggering deferred tasks

Additionally this repository will spin up an additional local Postgres instance running aat port `5433`.

2. Verify that all 5 Docker containers were created by running `docker ps`.

Note: Running `astro dev start` will start your project with the Airflow Webserver exposed at port `8080` and Postgres exposed at port `5432` and `5433`. If you already have one of those ports allocated, you can either [stop your existing Docker containers or change the port](https://docs.astronomer.io/astro/test-and-troubleshoot-locally#ports-are-not-available).

3. Access the Airflow UI for your local Airflow project. To do so, go to `http://localhost:8080/` and log in with `admin` for both your Username and Password.

Deploy Your Project to Astronomer
=================================

If you have an Astronomer account, pushing code to a Deployment on Astronomer is simple. For deploying instructions, refer to Astronomer documentation: https://docs.astronomer.io/cloud/deploy-code/

Contact
=======

The Astronomer CLI is maintained with love by the Astronomer team. To report a bug or suggest a change, reach out to our support.