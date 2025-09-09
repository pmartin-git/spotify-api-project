/*
Override the default "generate_schema_name" macro so that:
  * When dbt is run in production, models are built in the provided "custom_schema_name" schema, 
    rather than in the schema {{ default_schema }}_{{ custom_schema_name }}.
  * When local users build models with dbt, the model is built in the "local_dbt_builds" schema.
*/

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if target.name == "prod" -%}

        {%- if custom_schema_name is none -%}

            {{ default_schema }}

        {%- else -%}

            {{ custom_schema_name | trim }}

        {%- endif -%}
    
    {%- else -%}

            {{ "local_dbt_builds" | trim}}
        
    {%- endif -%}

{%- endmacro %}