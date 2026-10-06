{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if custom_schema_name is none -%}
        {{ target.schema }}

    {%- elif custom_schema_name == 'analytics' -%}
        analytics

    {%- elif custom_schema_name == 'staging' -%}
        staging

    {%- else -%}
        {{ custom_schema_name }}

    {%- endif -%}

{%- endmacro %}