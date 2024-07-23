{%- macro bridge_waterfall(period, date_field, dimension, metric, model=None, cte=None, filter_dimensions=None, start_date=None) -%}

{%- if target.type == 'bigquery' -%}
    {% if period == 'day' or period == 'week' %}
        {% set prefix = '%m-%d' %}
    {% elif period == 'month' %}
        {% set prefix = '%Y-%m' %}
    {% elif period == 'quarter' %}
        {% set prefix = '%Y' %}
    {% elif period == 'year' %}
        {% set prefix = '%Y' %}
    {% else %}
        {% set prefix = 'ERROR' %}
    {% endif %}
{% else %}
    {% if period == 'day' or period == 'week' %}
        {% set prefix = 'MM-DD' %}
    {% elif period == 'month' %}
        {% set prefix = 'YYYY-MM' %}
     {% elif period == 'quarter' %}
        {% set prefix = 'YYYY' %} {# The quarter is handled differently than the other periods. #}
     {% elif period == 'year' %}
        {% set prefix = 'YYYY' %}
     {% else %}
        {% set prefix = 'ERROR' %}
    {% endif %}
{%- endif -%}

{%- if target.type == 'bigquery' -%}
    {% set quarter_date_part = "extract(quarter from waterfall_date_at)" %}
    {% set date_prefix = "format_timestamp('" + prefix +"', waterfall_date_at)" %}
    {% set full_date_char_rc = "format_timestamp('%Y-%m', rc.waterfall_date_at)" %}
    {% set full_date_char = "format_timestamp('%Y-%m', waterfall_date_at)" %}
{%- else %}
    {# This should cover snowflake, redshift, and postgres #}
    {% set quarter_date_part = "date_part('quarter', waterfall_date_at)" %}
    {% set date_prefix = "to_char(waterfall_date_at, '" + prefix +"')" %}
    {% set full_date_char_rc = "to_char(rc.waterfall_date_at, 'YYYY-MM-DD')" %}
    {% set full_date_char = "to_char(waterfall_date_at, 'YYYY-MM-DD')" %}
{%- endif -%}


{%- if model -%}with {% endif %} {{ metric }}_by_{{ period }}_bu as (
    select
          {{ dbt.date_trunc(period, date_field) }} as waterfall_date_at
         , {{ dimension }}
            {%- if filter_dimensions %}
            {%- for filter_dim in filter_dimensions %}
         , {{ filter_dim }}
            {%- endfor %}
            {%- endif %}
         , sum({{ metric }})  as {{ metric }}
    from {% if model -%}{{ ref(model) }}{% else %}{{ cte }}{% endif %}
    {%- if start_date is not none %}
    where date({{ date_field }}) between '{{ start_date }}' and {{ dbt.dateadd(datepart='second',
                                                                               interval=-1,
                                                                               from_date_or_timestamp=dbt.date_trunc(period, dbt.current_timestamp())) }}
    {%- else %}
    where date({{ date_field }}) < {{ dbt.dateadd(datepart='second',
                                                  interval=-1,
                                                  from_date_or_timestamp=dbt.date_trunc(period, dbt.current_timestamp())) }}
    {% endif %}
group by waterfall_date_at
    , {{ dimension }}
    {%- if filter_dimensions %}
    {%- for dimension in filter_dimensions %}
    , {{ dimension }}
    {%- endfor %}
    {%- endif %}
    )

, {{ metric }}_changes as (
select
    waterfall_date_at
        , {{ dimension }}
        {%- if filter_dimensions %}
        {%- for filter_dim in filter_dimensions %}
            , {{ filter_dim }}
        {%- endfor %}
        {%- endif %}
        , {{ metric }}                                                         as current_period_{{ metric }}
        , lag({{ metric }})
           over (partition by {{ dimension }}
                            {%- if filter_dimensions %}
                            {%- for filter_dim in filter_dimensions %}
                                , {{ filter_dim }}
                            {%- endfor %}
                            {%- endif %}
                 order by waterfall_date_at desc) as previous_period_{{ metric }}
        , previous_period_{{ metric }} - {{ metric }}                          as {{ metric }}_change
from {{ metric }}_by_{{ period }}_bu
    )

, {{ period }}_totals as (
select
    waterfall_date_at
    {%- if filter_dimensions %}
    {%- for filter_dim in filter_dimensions %}
        , {{ filter_dim }}
    {%- endfor %}
    {%- endif %}
    , sum({{ metric }}) as total_{{ metric }}
from {{ metric }}_by_{{ period }}_bu
group by waterfall_date_at
    {%- if filter_dimensions %}
    {%- for filter_dim in filter_dimensions %}
        , {{ filter_dim }}
    {%- endfor %}
    {%- endif %}
    )
-- These prefixes
 , hidden_prefixes as (
select 1 period , '\u200B' as hidden_value union all
select 2 period , '\u200C' as hidden_value union all
select 3 period , '\u200D' as hidden_value union all
select 4 period , '\u200E' as hidden_value union all
select 5 period , '\u200F' as hidden_value union all
select 6 period , '\u2060' as hidden_value union all
select 7 period , '\u202A' as hidden_value union all
select 8 period , '\u202B' as hidden_value union all
select 9 period , '\u202C' as hidden_value union all
select 10 period , '\u202D' as hidden_value union all
select 11 period , '\u202E' as hidden_value union all
select 12 period , '\u202F' as hidden_value union all
select 13 as period , '\u200B\u200B' as hidden_value union all
select 14 as period , '\u200C\u200C' as hidden_value union all
select 15 as period , '\u200D\u200D' as hidden_value union all
select 16 as period , '\u200E\u200E' as hidden_value union all
select 17 as period , '\u200F\u200F' as hidden_value union all
select 18 as period , '\u2060\u2060' as hidden_value union all
select 19 as period , '\u202A\u202A' as hidden_value union all
select 20 as period , '\u202B\u202B' as hidden_value union all
select 21 as period , '\u202C\u202C' as hidden_value union all
select 22 as period , '\u202D\u202C' as hidden_value union all
select 23 as period , '\u202E\u202E' as hidden_value union all
select 24 as period , '\u202F\u202F' as hidden_value union all
select 13 as period , '\u200B\u200B\u200D' as hidden_value union all
select 14 as period , '\u200C\u200C\u200D' as hidden_value union all
select 15 as period , '\u200D\u200C\u200E' as hidden_value union all
select 16 as period , '\u200E\u200E\u200D' as hidden_value union all
select 17 as period , '\u200F\u200F\u200D' as hidden_value union all
select 18 as period , '\u2060\u2060\u200D' as hidden_value union all
select 19 as period , '\u202A\u202A\u200D' as hidden_value union all
select 20 as period , '\u202B\u202B\u200D' as hidden_value union all
select 21 as period , '\u202C\u202C\u200D' as hidden_value union all
select 22 as period , '\u202D\u202C\u200C' as hidden_value union all
select 23 as period , '\u202E\u202E\u200D' as hidden_value union all
select 24 as period , '\u202F\u202F\u200D' as hidden_value union all
select 25 as period , '\u200B\u200B\u200B' as hidden_value union all
select 26 as period , '\u200C\u200C\u200C' as hidden_value union all
select 27 as period , '\u200D\u200D\u200D' as hidden_value union all
select 28 as period , '\u200E\u200E\u200E' as hidden_value union all
select 29 as period , '\u200F\u200F\u200F' as hidden_value union all
select 30 as period , '\u2060\u2060\u2060' as hidden_value union all
select 31 as period , '\u202A\u202A\u202A' as hidden_value union all
select 32 as period , '\u202B\u202B\u202B' as hidden_value union all
select 33 as period , '\u202C\u202C\u202C' as hidden_value union all
select 34 as period , '\u202D\u202D\u202D' as hidden_value union all
select 35 as period , '\u202E\u202E\u202E' as hidden_value union all
select 36 as period , '\u202F\u202F\u202F' as hidden_value union all
select 37 as period , '\u200B\u200B\u200B\u200B' as hidden_value union all
select 38 as period , '\u200C\u200C\u200C\u200C' as hidden_value union all
select 39 as period , '\u200D\u200C\u200D\u200C' as hidden_value union all
select 40 as period , '\u200E\u200E\u200E\u200E' as hidden_value union all
select 41 as period , '\u200F\u200F\u200F\u200F' as hidden_value union all
select 42 as period , '\u2060\u2060\u2060\u2060' as hidden_value union all
select 43 as period , '\u202A\u202A\u202A\u202A' as hidden_value union all
select 44 as period , '\u202B\u202B\u202B\u202B' as hidden_value union all
select 45 as period , '\u202C\u202C\u202C\u202C' as hidden_value union all
select 46 as period , '\u202D\u202D\u202D\u202D' as hidden_value union all
select 47 as period , '\u202E\u202E\u202E\u202E' as hidden_value union all
select 48 as period , '\u202F\u202F\u202F\u202F\u200F' as hidden_value union all
select 49 as period , '\u202F\u202F\u202F\u202F\u2060' as hidden_value union all
select 50 as period , '\u202F\u202F\u202F\u202F\u202A' as hidden_value union all
select 51 as period , '\u202F\u202F\u202F\u202F\u202B' as hidden_value union all
select 52 as period , '\u202F\u202F\u202F\u202F\u202C' as hidden_value union all
select 53 as period , '\u202F\u202F\u202F\u202F\u202D' as hidden_value
    )

, waterfall_data as (
select
    {{ dbt.date_trunc(period, 'rc.waterfall_date_at') }}          as waterfall_date_at
    -- Important, no filter dimensions in the sort order value. That will break the script when run as it will create
    -- unique rows.
    , {{ full_date_char_rc }}
    || '-'
    || lpad((rank() over
        (partition by rc.{{ dimension }}
        order by rc.waterfall_date_at, rc.{{ dimension }}) + 1)::varchar, 3, '0')    as sort_order
    , hp.hidden_value || rc.{{ dimension }}                       as label
    -- , rc.{{ dimension }}
    {%- if filter_dimensions %}
    {%- for filter_dim in filter_dimensions %}
        , rc.{{ filter_dim }}
    {%- endfor %}
    {%- endif %}
    , coalesce(sum(rc.{{ metric }}_change)
        over (partition by rc.waterfall_date_at
                        {%- if filter_dimensions %}
                        {%- for filter_dim in filter_dimensions %}
                            , rc.{{ filter_dim }}
                        {%- endfor %}
                        {%- endif %}
              order by rc.{{ dimension }}
              rows between unbounded preceding and 1 preceding)
        , 0) + mt.total_{{ metric }}                               as bar_start
    , false                                                        as is_total
    , rc.{{ metric }}_change                                       as value
from {{ metric }}_changes as rc
    -- Join in the month level totals.
    left join {{ period }}_totals as mt on rc.waterfall_date_at = mt.waterfall_date_at
        {%- if filter_dimensions %}
        {%- for filter_dim in filter_dimensions %}
        and rc.{{ filter_dim }} = mt.{{ filter_dim }}
        {%- endfor %}
        {%- endif %}
    -- Join to the hidden chars based on the period value. This will evaluate to something like
    -- month(rc.waterfall_date_at) = hp.period
    left join hidden_prefixes as hp on {{ period }}(rc.waterfall_date_at) = hp.period
-- This filter is configured so that the total for the final {{ period }} will be in the data, but the period change components will not.
where rc.waterfall_date_at < {{ dbt.date_trunc(period, dbt.dateadd('month', -1, dbt.current_timestamp())) }}

    -- date_trunc('{{ period }}', dateadd('days', -1, date_trunc('{{ period }}', dbt.current_timestamp())))

union all

select
    waterfall_date_at
    , {{ full_date_char }} || '-' || '001'                     as sort_order
        {%- if period == 'quarter' -%}
        , {{ date_prefix }} || ' Q' || {{ quarter_date_part }} as label
        {%- else %}
        , {{ date_prefix }}                                    as label
        {%- endif %}
    --, {{ dimension }}
    {%- if filter_dimensions %}
    {%- for dimension in filter_dimensions %}
        , {{ dimension }}
    {%- endfor %}
    {%- endif %}
    , 0                                                        as bar_start
    , true                                                     as is_total
    , sum(current_period_{{ metric }})                         as value
from {{ metric }}_changes
group by waterfall_date_at
        , sort_order
        , label
        --, {{ dimension }}
        {%- if filter_dimensions %}
        {%- for filter_dim in filter_dimensions %}
        , {{ filter_dim }}
        {%- endfor %}
        {%- endif %}
        , bar_start
        , is_total

    )
, final as (
-- Create the total bar components
select
    waterfall_date_at
    , sort_order
    , label
    -- , {{ dimension }}
    {%- if filter_dimensions %}
    {%- for filter_dim in filter_dimensions %}
    , {{ filter_dim }}
    {%- endfor %}
    {%- endif %}
    , is_total
    , 'Total' as bar_type
    , case
        when is_total then value
    end       as value
from waterfall_data

union all

-- Create the hidden "white" bar components
select
    waterfall_date_at
    , sort_order
    , label
    -- , {{ dimension }}
    {%- if filter_dimensions %}
    {%- for filter_dim in filter_dimensions %}
    , {{ filter_dim }}
    {%- endfor %}
    {%- endif %}
    , is_total
    , 'Hidden' as bar_type
    , case when is_total then null
        when value < 0 then bar_start + value -- value is negative here, so reduce height
        else bar_start
    end as value
from waterfall_data

union all

-- Gain bars
select
    waterfall_date_at
    , sort_order
    , label
    -- , {{ dimension }}
    {%- if filter_dimensions %}
    {%- for filter_dim in filter_dimensions %}
    , {{ filter_dim }}
    {%- endfor %}
    {%- endif %}
    , is_total
    , 'Gain' as bar_type
    , case
        when is_total then null
        when value < 0 then null
        else value
    end as value
from waterfall_data

union all

-- Loss bars
select
    waterfall_date_at
    , sort_order
    , label
    -- , {{ dimension }}
    {%- if filter_dimensions %}
    {%- for filter_dim in filter_dimensions %}
    , {{ filter_dim }}
    {%- endfor %}
    {%- endif %}
    , is_total
    , 'Loss' as bar_type
    , case
    when is_total then null
    when value > 0 then null
    -- Set the bar value to positive so it will stack right.
    else - value
    end as value
from waterfall_data

    )

select
    waterfall_date_at
    , sort_order
    , label
    -- , {{ dimension }}
    {%- if filter_dimensions %}
    {%- for filter_dim in filter_dimensions %}
    , {{ filter_dim }}
    {%- endfor %}
    {%- endif %}
    , is_total
    , bar_type
    , value

from final
where value is not null
order by sort_order, bar_type desc

{%- endmacro -%}