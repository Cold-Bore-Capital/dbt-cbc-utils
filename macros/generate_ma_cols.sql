{#
 Generates a series of moving average columns for a given set of metric columns and dimensions.
 #}
{%- macro generate_ma_cols(column_names, date_field, dimensions, ma_days, grain, add_coalesce=true) -%}
{%- set space = ' ' -%}
{%- for column_name in column_names -%}
    {# The column name might start with an alias, like apts.my_column #}
    {%- set column_name_parts = column_name.split('.') -%}
    {%- if column_name_parts | length == 2 -%}
        {%- set clean_column = column_name_parts[1] -%}
    {%- else -%}
        {%- set clean_column = column_name -%}
    {%- endif -%}

    {%- if loop.index > 1 -%}
        {%- set comma = "\n, " -%}
    {%- endif -%}
    {%- for days in ma_days -%}
        {%- set column_alias -%}
            {{ clean_column }}_{{ days }}{{ grain }}_ma
        {%- endset -%}
        {{- comma -}}
        {{- space -}}avg(
        {%- if add_coalesce == true -%}
            coalesce(
        {%- endif -%}
        {{ column_name }}
        {%- if add_coalesce == true -%}
            ,0)) over (
        {%- endif -%}
        {%- if dimensions -%}
            partition by {{ dimensions | join(", ") }}
        {%- endif -%}
        {{- space -}}order by {{ date_field }} rows between {{ days - 1}} preceding and current row) as {{ column_alias }}
    {%- endfor -%}

{%- endfor -%}
{%- endmacro -%}