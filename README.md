## Spotify API Project 
##### by Peter Martin (09.08.2025)

&nbsp;
### Overview:
This repository contains a project to retrieve and analyze data relating to my personal public Spotify playlists. The goal was to expand my learning while discovering fun facts about my music listening habits.

The code in this repo is written to accomplish the following:
 - Pull data from the Spotify Web API through client credential authorization
 - Process the raw API data into tabular forms
 - Load the processed tabular data into a cloud Postgres database
 - Create a dbt project for further transformation, analysis, and testing of data
 - Sync repository code to an EC2 instance acting as an Airflow server
 - Run Airflow DAGs on a schedule from EC2 instance / Airflow server

Code snippets that are not part of the repo code itself are included in various README.md files throughout for posterity and personal reference.

&nbsp;
### Results:

Some basic results can be seen here: https://docs.google.com/spreadsheets/d/1Ij26YOzTuDL3AHKUA4rTUvFLf4R1w9BN/edit?usp=drive_link&ouid=118279630434716919521&rtpof=true&sd=true

Refs: \
[1] https://rowzero.io/blog/connect-postgresql-to-microsoft-excel

&nbsp;
### Code Summary:

This pull request provides a simple summary of development: https://github.com/pmartin-git/spotify-api-project/pull/1/files

&nbsp;
### Tech Details:
All tools used for this project are freely available either as open source technologies, free for a limited period of time (6-12 months), or as a paid service that comes with initial credits.

* **Windows Subsystem for Linux (WSL)**: \
  Since I own a Windows machine, I developed my project within an Ubuntu-22.04 WSL instance.

  > $ Enable-WindowsOptionalFeature -Online -FeatureName Microsoft-Windows-Subsystem-Linux \
  > $ wsl --install \
  > $ wsl --set-version Ubuntu-22.04 2

  Refs: \
  [1] https://learn.microsoft.com/en-us/windows/wsl/install

* **Pyenv + Pyenv-virtualenv**: \
  Pyenv is used for Python version management and virtual environment creation. For this project the local Python version is set to 3.11.8. A virtual environment was created for using Airflow locally, despite using Docker to run Airflow; this was only to have Intellisence and auto-complete enabled in VS Code while writing DAG definitions.

  Installed via Homebrew:
  > $ /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)" \
  > $ echo >> /home/pmartin/.bashrc \
  > $ echo 'eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"' >> /home/pmartin/.bashrc \
  > $ eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)" \
  > $ sudo apt-get install build-essential \
  > $ sudo apt-get update \

  > $ brew install pyenv \
  > $ brew install pyenv-virtualenv \
  > $ echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc \
  > $ echo '[[ -d $PYENV_ROOT/bin ]] && export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc \
  > $ echo 'eval "$(pyenv init - bash)"' >> ~/.bashrc \

  Refs: \
  [1] https://brew.sh/ \
  [2] https://github.com/pyenv/pyenv?tab=readme-ov-file#installation \
  [3] https://github.com/pyenv/pyenv?tab=readme-ov-file#b-set-up-your-shell-environment-for-pyenv \

* **Poetry**: \
  Poetry is used for managing Python packages necessary for this project's Airflow + dbt implementations. Poetry figures out the versions to use to avoid package conflicts and installs them in a virtual environment. Not a necessary tool for this small project, but good to know how to use.

  Locally installed with pipx:
  > $ sudo apt update \
  > $ sudo apt install pipx \
  > $ pipx ensurepath

  > $ pipx install poetry

  Refs: \
  [1] https://pipx.pypa.io/stable/installation/ \
  [2] https://python-poetry.org/docs/

* **Docker**: \
  Docker is used for running Airflow both locally and on an EC2 instance. The image is built off the official Airflow 3.0.4 image, but with additional configs for copying in dbt project and using Poetry for Python package management. Services are managed with a compose file built off the official Airflow compose file, but using a LocalExecutor and with additional configs relating to dbt.

  Refs: \
  [1] https://www.udemy.com/course/learn-docker/ \
  [2] https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#fetching-docker-compose-yaml

* **Amazon RDS**: \
  Rather than use Docker for creating a database, or installing one directly, Amazon RDS is used for hosting a cloud Postgres database to store project data (in order to more closely resemble a real-world project).

  Refs: \
  [1] https://aws.amazon.com/blogs/database/managing-postgresql-users-and-roles/ \
  [2] https://popsql.com/learn-sql/postgresql/how-to-query-a-json-column-in-postgresql

* **Airflow**: \
  Airflow is the ELT tool for pulling data from the Spotify Web API and loading it into Postgres. Profile credentials within Docker managed using environment variables. Connection to Postgres and Web API client credentials managed through Airflow UI.

  Refs: \
  [1] https://www.udemy.com/course/the-complete-hands-on-course-to-master-apache-airflow/

* **Amazon S3 Storage**: \
  Using S3 wasn't necessary to make this project work, but I wanted to learn more about it. S3 is used for storing raw and processed data retrieved from the API.

  Refs: \
  [1] https://www.radishlogic.com/aws/how-to-load-a-json-file-from-s3-to-a-python-dictionary-using-boto3/ \
  [2] https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_parquet.html

* **dbt**: \
  dbt is the primary transformation tool for building SQL tables/views and for basic data tests/observability. Profile credentials within Docker managed using environment variables.

  Refs: \
  [1] https://github.com/dbt-labs/dbt-codegen \
  [2] https://docs.getdbt.com/docs/build/custom-schemas \
  [3] https://docs.getdbt.com/docs/build/custom-aliases

* **GitHub Actions**: \
  GitHub Actions is the CI/CD service used to deploy code from the main branch in GitHub to an EC2 instance. Connection to EC2 is managed via SSH with credentials stored as GitHub secrets.

  Refs: \
  [1] https://www.udemy.com/course/github-actions-the-complete-guide/

* **Rsync**: \
  Rsync is the specific program used in GitHub Action workflow for syncing code from GitHub to EC2.

  Refs: \
  [1] https://benhoskins.dev/github-actions-deploy-with-rsync/

* **Amazon EC2**: \
  An EC2 instance (*Ubuntu-22.04, m7i-flex.large*) is created to serve as the "production" Airflow server for this project. Since this instance costs credits with AWS, I can only host it for a short period of time, but the important thing was getting it to work and sync with GitHub.

  Install Docker on EC2 from repository bundle:
  > $ sudo apt update && sudo apt install ca-certificates curl gnupg \
  > $ sudo install -m 0755 -d /etc/apt/keyrings \
  > $ curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
  > $ echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null \
  > $ sudo apt update && sudo apt install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin \
  > $ sudo usermod -aG docker $USER \
  > $ docker run hello-world

  Refs: \
  [1] https://medium.com/@springmusk/why-isnt-it-just-sudo-apt-install-docker-bfe55d73fcf9

&nbsp;
### Potential Improvements:

* **Access private profile data**: \
  This project only uses data accessible through client credential authorization--such data is limited to publicly available resources (e.g. general artist/track data, or public playlist data). Since my public playlists are curated by me, I can still learn things about my taste in music this way.

  To access private profile data (e.g. most frequently played artists/tracks), the authorization process requires the user to explicitly grant access through a prompt; afterwards an access token is provided using a redirect URI which must be available either through HTTPS protocol, or HTTP protocol if the redirect URI is localhost (but that method seems non-secure).

  A potential future improvement might be to use Flask to create a valid redirect URI, enabling further data access regarding my listening habits.

* **Custom Airflow operators**: \
  It could make sense to write operators for pulling playlist and artist data from the API rather than defining most of the logic within specific tasks in a DAG. The advantage to the current approach is the ease of transfering XCom data between tasks.

* **Security improvements**: \
  Project currently relies heavily on environment variables, Airflow variables/connections, and GitHub secrets for implementation. While this provides a level of security, a future improvement may be to figure out if services exist to create more secure connections/configs.

* **CI/CD improvements**: \
  Current CI/CD setup only syncs code from GitHub to EC2; it does not run any tests on the code. A future improvement may be to add standard tests before deployment.
