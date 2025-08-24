/*
Override the default "generate_alias_name" macro so that when local users build models with dbt, 
the model name is prefixed with the user's account name to distinguish from other user models
within the "local_dbt_builds" schema.
*/

{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {%- if target.name == "prod" -%}

        {%- if custom_alias_name -%}

            {{ custom_alias_name | trim }}

        {%- elif node.version -%}

            {{ return((node.name ~ "_v" ~ (node.version | replace(".", "_"))) | trim) }}

        {%- else -%}

            {{ node.name }}

        {%- endif -%}

    {%- else -%}

        {%- if custom_alias_name -%}

            {{ [target.user, custom_alias_name] | join("__") | trim }}

        {%- elif node.version -%}

            {{ return((( [target.user, node.name] | join("__")) ~ "_v" ~ (node.version | replace(".", "_"))) | trim) }}
        
        {%- else -%}

            {{ [target.user, node.name] | join("__") | trim }}

        {%- endif -%}

    {%- endif -%}

{%- endmacro %}