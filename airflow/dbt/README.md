====dbt environment for Spotify API project.====

global_dbt_packages: In case multiple dbt projects are created for this overall project, this directory is meant to make
all packages available to all projects.

global_dbt_macros: In case multiple dbt projects are created for this overall project, this directory is meant to make all
custom macros available to all projects.

DDL for creating local_dbt_user:
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
CREATE ROLE localdbt;
GRANT CONNECT ON DATABASE postgres TO localdbt;
GRANT USAGE ON SCHEMA local_dbt_builds, spotify_sources, spotify_staging, spotify_intermediate, spotify_presentation TO localdbt;
GRANT CREATE ON SCHEMA local_dbt_builds TO localdbt;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA local_dbt_builds TO localdbt;
ALTER DEFAULT PRIVILEGES IN SCHEMA local_dbt_builds GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO localdbt;
GRANT SELECT ON ALL TABLES IN SCHEMA spotify_sources, spotify_staging, spotify_intermediate, spotify_presentation TO localdbt;
ALTER DEFAULT PRIVILEGES IN SCHEMA spotify_sources, spotify_staging, spotify_intermediate, spotify_presentation GRANT SELECT ON TABLES TO localdbt;
CREATE USER local_dbt_user WITH PASSWORD ****;
GRANT localdbt TO local_dbt_user;