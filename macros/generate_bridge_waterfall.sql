{%- macro bridge_waterfall(period, date_field, dimension, metric, model=None, cte=None, start_date=None) -%}

{%- if period == 'day' or period == 'week' -%}
{%- set prefix = 'MM-DD' -%}
{%- elif period == 'month' -%}
{%- set prefix = 'YYYY-MM' -%}
{%- elif period == 'quarter' -%}
{%- set prefix = 'YYYY' -%} {# The quarter is handled differently than the other periods. #}
{%- elif period == 'year' -%}
{%- set prefix = 'YYYY' -%}
{%- else -%}
{%- set prefix = 'ERROR' -%}
{%- endif -%}

{%- if model -%}with {% endif %} {{ metric }}_by_{{ period }}_bu as (
    select
        date_trunc('{{ period }}', {{ date_field }}) as waterfall_date_at
         , {{ dimension }}
         , sum({{ metric }})  as {{ metric }}
    from {% if model -%}{{ ref(model) }}{% else %}{{ cte }}{% endif %}
    {%- if start_date is not none %}
    where date({{ date_field }}) between '{{ start_date }}' and dateadd('days', -1, date_trunc('{{ period }}', current_date))
    {%- else %}
    where date({{ date_field }}) < dateadd('days', -1, date_trunc('{{ period }}', current_date))
    {% endif %}
group by 1, 2
    )

, {{ metric }}_changes as (
select
    waterfall_date_at
        , {{ dimension }}
        , {{ metric }}                                                         as current_period_{{ metric }}
        , lag({{ metric }})
           over (partition by {{ dimension }} order by waterfall_date_at desc) as previous_period_{{ metric }}
        , previous_period_{{ metric }} - {{ metric }}                          as {{ metric }}_change
from {{ metric }}_by_{{ period }}_bu
    )

, {{ period }}_totals as (
select
    waterfall_date_at
    , sum({{ metric }}) as total_{{ metric }}
from {{ metric }}_by_{{ period }}_bu
group by 1
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
    date_trunc('{{ period }}', rc.waterfall_date_at) as waterfall_date_at
    , to_char(rc.waterfall_date_at, 'YYYY-MM-DD')
    || '-'
    || lpad((row_number() over (order by rc.waterfall_date_at, rc.{{ dimension }}) + 1)::varchar, 3, '0') as sort_order
    , hp.hidden_value || rc.{{ dimension }}                                    as label
    , coalesce(sum(rc.{{ metric }}_change)
        over (partition by rc.waterfall_date_at order by rc.{{ dimension }} rows between unbounded preceding and 1 preceding)
        , 0
    ) + mt.total_{{ metric }}                                                                             as bar_start
    , false                                                                                               as is_total
    , rc.{{ metric }}_change                                                                              as value
from {{ metric }}_changes as rc
    left join {{ period }}_totals as mt on rc.waterfall_date_at = mt.waterfall_date_at
    -- Join to the hidden chars based on the period value. This will evaluate to something like
    -- month(rc.waterfall_date_at) = hp.period
    left join hidden_prefixes as hp on {{ period }}(rc.waterfall_date_at) = hp.period
-- This filter is configured so that the total for the final {{ period }} will be in the data, but the period change components will not.
where rc.waterfall_date_at < date_trunc('{{ period }}', dateadd('days', -1, date_trunc('{{ period }}', current_date)))

union all

select
    waterfall_date_at
    , to_char(waterfall_date_at, 'YYYY-MM-DD') || '-' || '001' as sort_order
    {%- if period == 'quarter' -%}
    , to_char(waterfall_date_at, '{{ prefix }}') || ' Q' || quarter(waterfall_date_at)               as label
    {%- else %}
    , to_char(waterfall_date_at, '{{ prefix }}')               as label
    {%- endif %}
    , 0                                                        as bar_start
    , true                                                     as is_total
    , sum(current_period_{{ metric }})                         as value
from {{ metric }}_changes
group by all

    )

-- Create the total bar components
select
    waterfall_date_at
     , sort_order
     , label
     , is_total
     , 'Total' as bar_type
     , case
       when is_total then value
       else null
    end       as value
from waterfall_data

union all

-- Create the hidden "white" bar components
select
    waterfall_date_at
     , sort_order
     , label
     , is_total
     , 'Hidden' as bar_type
     , case when is_total then null
       when value < 0 then bar_start + value -- value is negative here, so reduce height
       else bar_start
    end         as value
from waterfall_data

union all

-- Gain bars
select
    waterfall_date_at
     , sort_order
     , label
     , is_total
     , 'Gain' as bar_type
     , case
       when is_total then null
       when value < 0 then null
       else value
    end       as value
from waterfall_data

union all

-- Loss bars
select
    waterfall_date_at
     , sort_order
     , label
     , is_total
     , 'Loss' as bar_type
     , case
       when is_total then null
       when value > 0 then null
    -- Set the bar value to positive so it will stack right.
       else -value
    end       as value
from waterfall_data
where is_total = false
order by sort_order, bar_type desc

{%- endmacro -%}