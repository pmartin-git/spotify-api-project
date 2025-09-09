**Postgres DDL for creating local dbt user (vs. production user for CI/CD)**
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
CREATE ROLE localdbt;
GRANT CONNECT ON DATABASE postgres TO localdbt;
GRANT USAGE ON SCHEMA local_dbt_builds, spotify_sources, spotify_staging, spotify_intermediate, spotify_presentation TO localdbt;
GRANT CREATE ON SCHEMA local_dbt_builds TO localdbt;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA local_dbt_builds TO localdbt;
ALTER DEFAULT PRIVILEGES IN SCHEMA local_dbt_builds GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO localdbt;
GRANT SELECT ON ALL TABLES IN SCHEMA spotify_sources, spotify_staging, spotify_intermediate, spotify_presentation TO localdbt;
ALTER DEFAULT PRIVILEGES IN SCHEMA spotify_sources, spotify_staging, spotify_intermediate, spotify_presentation GRANT SELECT ON TABLES TO localdbt;
CREATE USER <local_dbt_user> WITH PASSWORD <password>;
GRANT localdbt TO <local_dbt_user>;