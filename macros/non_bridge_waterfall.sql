{% macro non_bridge_waterfall(primary_dimension, metrics, cte=None, model=None, filter_dimensions=None, sort_order=None) %}

{%- if model -%}
with initial as (
{%- else %}
, initial as (
{%- endif %}
    select
         {{ primary_dimension }}
         {%- if filter_dimensions %}
             {%- for dimension in filter_dimensions %}
             , {{ dimension }}
             {%- endfor %}
         {%- endif %}
         {%- if sort_order %}
         , {{ sort_order }} as sort_order
         {%- else -%}
          , row_number() over (order by {{ primary_dimension }}) as sort_order
         {%- endif %}
         {%- for metric in metrics %}
         , sum({{ metric }}) as {{ metric }}
         {%- endfor %}
    from
        {%- if model %}
        {{ ref(model) }}
        {%- else %}
        {{ cte }}
        {%- endif %}
    group by all
)
{%- if filter_dimensions %}
  ,  filter_dimensions as (
    select distinct
        {{ filter_dimensions[0] }}
            {%- for dimension in filter_dimensions[1:] %}
                  , {{ dimension }}
            {%- endfor %}
    from initial
)
{%- endif %}
  , primary_dimension as (
    select distinct
        {{ primary_dimension }}
          , sort_order
    from initial
)
  , cj as (
    select
        {{ primary_dimension }}
        {%- if filter_dimensions %}
            {%- for dimension in filter_dimensions %}
            , {{ dimension }}
            {%- endfor %}
        {%- endif %}
            , sort_order
    from primary_dimension
             {%- if filter_dimensions %}
             cross join filter_dimensions
             {%- endif %}
)
  , segment_totals as (
    select
        cj.{{ primary_dimension }}
        {%- if filter_dimensions %}
        {%- for dimension in filter_dimensions %}
         , cj.{{ dimension }}
        {%- endfor %}
        {%- endif %}
         , cj.sort_order
        {%- for metric in metrics %}
         , i.{{ metric }}
         , sum(i.{{ metric }})
            over (
            {%- if filter_dimensions %}
            partition by cj.{{ filter_dimensions[0] }}
            {%- for dimension in filter_dimensions[1:] %}
            , cj.{{ dimension }}
            {%- endfor %}
            {%- endif %}
           order by cj.sort_order rows between unbounded preceding and 1 preceding) as {{ metric }}_cummulative
         {%- endfor %}
    from cj
             left join initial as i on
                 cj.{{ primary_dimension }} = i.{{ primary_dimension }}
                     {%- if filter_dimensions %}
                     {%- for dimension in filter_dimensions %}
                     and cj.{{ dimension }} = i.{{ dimension }}
                    {%- endfor %}
                    {%- endif %}
)

  , total as (
    select
        {% if filter_dimensions -%}
        {{ filter_dimensions[0] }}
        {%- for dimension in filter_dimensions[1:] %}
         , {{ dimension }}
        {%- endfor %}
         ,
        {%- endif %}
        sum({{ metrics[0] }}) as {{ metrics[0] }}_grand_total
        {%- for metric in metrics[1:] %}
         , sum({{ metric }}) as {{ metric }}_grand_total
         {%- endfor %}
    from initial
    group by all
)

  , final as (
    select
        {{ primary_dimension }}
            {%- if filter_dimensions %}
            {%- for dimension in filter_dimensions %}
         , {{ dimension }}
            {%- endfor %}
            {%- endif %}
         , sort_order
         , '01 - Hidden'                      as bar_type
        {%- for metric in metrics %}
         , {{ metric }}_cummulative   as _{{ metric }}_value
        {%- endfor %}
    from segment_totals
    union all
    select
        {{ primary_dimension }}
            {%- if filter_dimensions %}
            {%- for dimension in filter_dimensions %}
         , {{ dimension }}
            {%- endfor %}
            {%- endif %}
         , sort_order
         , '02 - Gain'                      as bar_type
            {%- for metric in metrics %}
         , {{ metric }}      as _{{ metric }}_value
            {%- endfor %}
    from segment_totals
    union all
    select
        'Grand Total'          as {{ primary_dimension }}
        {%- if filter_dimensions %}
        {%- for dimension in filter_dimensions %}
         , {{ dimension }}
        {%- endfor %}
        {%- endif %}
         , 999                        as sort_order
         , '04 - Total'                    as bar_type
            {%- for metric in metrics %}
         , {{ metric }}_grand_total   as _{{ metric }}_value
            {%- endfor %}
    from total
)

select
    {{ primary_dimension }}
        {%- if filter_dimensions %}
        {%- for dimension in filter_dimensions %}
     , {{ dimension }}
        {%- endfor %}
        {%- endif %}
     , sort_order
      , bar_type
        {%- for metric in metrics %}
     , coalesce(_{{ metric }}_value,0) as {{ metric }}
        {%- endfor %}
from final
order by sort_order, bar_type, {{ primary_dimension }}
    {% if filter_dimensions -%}
       {%- for dimension in filter_dimensions %}
         , {{ dimension }}
        {%- endfor %}
    {%- endif %}
{% endmacro %}