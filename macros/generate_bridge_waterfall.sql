{%- macro bridge_waterfall(model, period, date_field, dimension, metric,  start_date=None) -%}

{%- if period == 'day' or period == 'week' -%}
{%- set prefix = 'MM-DD' -%}
{%- elif period == 'month' -%}
{%- set prefix = 'YY-MM' -%}
{%- elif period == 'quarter' -%}
{%- set prefix = 'YY' -%} {# The quarter is handled differently than the other periods. #}
{%- elif period == 'year' -%}
{%- set prefix = 'YY' -%}
{%- else -%}
{%- set prefix = 'ERROR' -%}
{%- endif -%}

with {{ metric }}_by_{{ period }}_bu as (
    select
        date_trunc('{{ period }}', {{ date_field }}) as waterfall_date_at
         , {{ dimension }}
         , sum({{ metric }})  as {{ metric }}
    from {{ ref(model) }}
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

, waterfall_data as (
select
    date_trunc('{{ period }}', rc.waterfall_date_at) as waterfall_date_at
    , to_char(rc.waterfall_date_at, 'YYYY-MM-DD')
    || '-'
    || lpad((row_number() over (order by rc.waterfall_date_at, rc.{{ dimension }}) + 1)::varchar, 3, '0') as sort_order
    {%- if period == 'quarter' -%}
    , to_char(rc.waterfall_date_at, '{{ prefix }}') || quarter(rc.waterfall_date_at) || '-' || rc.{{ dimension }}   as label
    {%- else %}
    , to_char(rc.waterfall_date_at, '{{ prefix }}') || '-' || rc.{{ dimension }}                                    as label
    {%- endif %}

    , coalesce(sum(rc.{{ metric }}_change)
        over (partition by rc.waterfall_date_at order by rc.{{ dimension }} rows between unbounded preceding and 1 preceding)
        , 0
    ) + mt.total_{{ metric }}                                                                             as bar_start
    , false                                                                                               as is_total
    , rc.{{ metric }}_change                                                                              as value
from {{ metric }}_changes as rc
    left join {{ period }}_totals as mt on rc.waterfall_date_at = mt.waterfall_date_at
-- This filter is configured so that the total for the final {{ period }} will be in the data, but the period change components will not.
where rc.waterfall_date_at < date_trunc('{{ period }}', dateadd('days', -1, date_trunc('{{ period }}', current_date)))

union all

select
    waterfall_date_at
    , to_char(waterfall_date_at, 'YYYY-MM-DD') || '-' || '001' as sort_order
    {%- if period == 'quarter' -%}
    , to_char(waterfall_date_at, '{{ prefix }}') || quarter(waterfall_date_at)               as label
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