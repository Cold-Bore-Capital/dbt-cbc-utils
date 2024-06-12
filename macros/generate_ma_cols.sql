{#
 Generates a series of moving average columns for a given set of metric columns and dimensions.
 #}
{%- macro generate_ma_colums(column_names, date_field, dimensions, ma_days, grain, add_coalesce=true, comma_at_end=true) -%}
{%- set space = ' ' -%}
{%- for column_name in column_names -%}
    {# used to detect the final col iteration to avoid a hanging comma #}
    {%- set final_column_name = column_names[-1] -%}
    {# The column name might start with an alias, like apts.my_column #}
    {%- set column_name_parts = column_name.split('.') -%}
    {%- if column_name_parts | length == 2 -%}
        {%- set clean_column = column_name_parts[1] -%}
    {%- else -%}
        {%- set clean_column = column_name -%}
    {%- endif -%}

    {%- if loop.index > 1 %}
        {%- if comma_at_end == false -%}
            {%- set comma = "\n, " -%}
        {%- endif -%}
    {%- endif %}
    {%- for days in ma_days -%}
        {%- if loop.index > 1 %}
            {%- if comma_at_end == false -%}
                {%- set comma = "\n, " -%}
            {%- endif -%}
        {%- endif %}
        {%- set column_alias -%}
            {{ clean_column }}_{{ days }}{{ grain }}_ma
        {%- endset -%}
        {%- if comma_at_end == false -%} {{- comma -}} {%- endif -%}
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
         {%- if comma_at_end == true -%}
            {%- if column_name == final_column_name and loop.last -%}
                {# For the final iteration, no comma. #}
                {%- set comma = "" -%}
            {%- else -%}
                {%- set comma = ",\n" -%}
            {%- endif -%}
            {{- comma -}}
        {%- endif -%}
    {%- endfor -%}

{%- endfor -%}
{%- endmacro -%}