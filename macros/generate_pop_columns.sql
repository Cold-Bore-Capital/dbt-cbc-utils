{%- macro generate_pop_columns(column_names, date_field, dimensions, look_back_values, grain, column_prefix=none, add_coalesce=true) -%}
{%- set space = ' ' -%}
{%- for column_name in column_names %}
    {%- if loop.index > 1 %}
        {%- set comma = "\n, " -%}
    {%- endif -%}
    {# The column name might start with an alias, like apts.my_column #}
    {%- set column_name_parts = column_name.split('.') -%}
    {%- if column_name_parts | length == 2 -%}
        {%- set clean_column = column_name_parts[1] -%}
    {%- else -%}
        {%- set clean_column = column_name -%}
    {%- endif -%}

    {%- for look_back_val in look_back_values %}
        {%- if loop.index > 1 %}
            {%- set comma = "\n, " -%}
        {%- endif %}
        {%- if column_prefix is not none -%}
            {%- set column_alias -%}
                {{ column_prefix }}_{{ clean_column }}_{{ look_back_val }}{{ grain }}
            {%- endset -%}
        {% else %}
            {%- set column_alias -%}
                {{ clean_column }}_{{ look_back_val }}{{ grain }}
            {%- endset -%}
         {%- endif -%}

         {{- comma }}
                {%- if add_coalesce == true -%}
                coalesce(
                {%- endif -%}
                lag({{ column_name }}, {{ look_back_val }}) over (
                {%- if dimensions -%}
                partition by {{ dimensions | join(", ") }}
                {%- endif -%}
                {{- space -}}order by {{ date_field }})
                {%- if add_coalesce == true -%}
                , 0)
                {%- endif %} as {{ column_alias }}
    {% endfor -%}
{%- endfor -%}

{%- endmacro  -%}