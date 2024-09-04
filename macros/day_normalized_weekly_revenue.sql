/*
Warning specific to RSP, do not use this macro for NBOT. NBOT cannot be calculated at the day level with the current data model.
The values to calculate NBOT would need to be moved from fct_jobs_by_week_mat to fct_schedules_mat, which would be
tricky, but should be possible with a window function.
*/

{%  macro day_normalized_weekly_revenue(date_field, metrics, dimensions=None, model=None, cte=None) %}

{% if model %}with{%- else %},{% endif %} s1 as (
    select
    {{ date_field }} as date_at
                 , dayofweek(date_at)        as dow
                 , month(date_at)            as month_val
                 , year(date_at)             as year_val
    {%- for dimension in dimensions %}
                 , {{ dimension }}
    {%- endfor %}
    {%- for metric in metrics %}
                 , sum({{ metric }}) as {{ metric }}_sum
    {%- endfor %}
    from {% if model %}{{ ref(model) }}{%- else %}{{ cte }}{% endif %}
    group by all
    order by 1 desc
    )
                 , s2 as (
    select
    year_val
                 , month_val
                 , dow
    {%- for dimension in dimensions %}
                 , {{ dimension }}
    {%- endfor %}
    {%- for metric in metrics %}
                 , avg({{ metric }}_sum) as avg_dow_{{ metric }}
    {%- endfor %}
    from s1
    group by all
    )
select date_from_parts(year_val, month_val, 1) as month_at
        {%- for dimension in dimensions %}
  , {{ dimension }}
        {%- endfor %}
        {% for metric in metrics %}
  , sum(avg_dow_{{ metric }}) as day_normalized_weekly_{{ metric }}
        {%- endfor %}
from s2
group by all
order by 1 desc, 2, 3

{% endmacro %}