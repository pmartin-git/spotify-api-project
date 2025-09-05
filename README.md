### Spotify API Project
# by Peter Martin (09.03.2025)


## Overview
This repository houses a project to retrieve and analyze data relating to my personal public Spotify playlists. The goal was to expand my learning while discovering fun tidbits about my music listening habits.

The code in this repo is written to accomplish the following:
 - Pull data from the Spotify Web API through client credential authorization
 - Process the raw API data into tabular forms
 - Load the processed tabular data into a cloud Postgres database
 - Create a dbt project for further transformation, analysis, and testing of data
 - Sync repository code to an EC2 instance acting as an Airflow server
 - Run Airflow DAGs on a schedule from EC2 instance / Airflow server


## Tech Details
All tools used for this project are freely available either as open source technologies or free for a limited period of time (6-12 months).

**Pyenv + Pyenv-virtualenv**: Pyenv is used for Python version management and virtual environment creation. For this project I set the local Python version to 3.11.8. A virtual environment was created for using Airflow locally, despite using Docker to run Airflow; this was only to have Intellisence and auto-complete enabled in VS Code while writing DAG definitions.

**Poetry**: Poetry is used for managing Python packages necessary for this project's Airflow + dbt implementations. Poetry is an great tool for this because it figures out the versions necessary to avoid package conflicts. Not necessary for a project this small, but good to know how to use for larger projects. Locally installed with pipx, but for the Docker image it is installed directly with pip.

**Docker**: Docker is used for running Airflow both locally and on an EC2 instance. The image is built off the official Airflow 3.0.4 image, but with additional configs for copying in dbt project and using Poetry for Python package management. Services are managed with a compose file built off the official Airflow compose file, but using a LocalExecutor and with additional configs relating to dbt.

**Amazon RDS**: Rather than use Docker for creating a database, or installing one directly, Amazon RDS is used for hosting a cloud Postgres database to store project data to more closely resemble a real-world project.

**Airflow**: Airflow is the ELT tool for pulling data from the Spotify Web API and loading it into Postgres. Profile credentials within Docker managed using environment variables. Connection to Postgres and Web API client credentials managed through Airflow UI.

**Amazon S3 Storage**: Using S3 wasn't necessary for making this project work, but I wanted to learn more about it. S3 is used for storing raw and processed data retrieved from the API.

**dbt**: dbt is the primary transformation tool for building SQL tables/views and for basic data tests/observability. Profile credentials within Docker managed using environment variables.

**GitHub**: GitHub stores the code for the project.

**GitHub Actions**: GitHub Actions is the CI/CD service used to deploy code from the main branch in GitHub to an EC2 instance. Connection to EC2 is managed via SSH with credentials stored as GitHub secrets.

**Rsync**: Rsync is the specific program used in GitHub Action workflow for syncing code from GitHub to EC2.

**Amazon EC2**: An EC2 instance (*Ubuntu-22.04, c7i-flex.large*) is created to serve as the "production" Airflow server for this project.


## Potential Improvements

**Access private profile data**: This project only uses data accessible from Spotify using client credential authorization--such data is limited to publicly available resources (e.g. general artist/track data, or public playlist data). Since my public playlists are curated by me, I can still learn things about my taste in music this way.

To access private profile data (e.g. most frequently played artists/tracks), the authorization process requires the user to explicitly grant access through a prompt; afterwards an access token is provided using a redirect URI which must be available either through HTTPS protocol, or HTTP protocol if the redirect URI is localhost (but that method seems non-secure).

A potential future improvement might be to use Flask to create a valid redirect URI, enabling further data access regarding my listening habits.

**Custom Airflow operators**: It might make sense to write operators for pulling playlist and artist data from the API rather than defining most of the logic within specific tasks in a DAG. The advantage to the current approach is the ease of transfering XCom data between tasks.