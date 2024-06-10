# CBC Utils

## Usage
Add the following to the packages.yml file in your dbt project.
```
  - git: "https://github.com/Cold-Bore-Capital/dbt-cbc-utils.git"
    revision: <get latest release>
```

## Macros

### `generate_ma_cols` - Generate Moving Average Columns

This macro is designed to create moving average columns over a given date column and dimensions. 

Example Usage
```sql
select
    dlcj.date_at
    , dlcj.location_id
    , dlcj.location_name
    , dlcj.network_name
    , coalesce(r.revenue_total,0)        as revenue
    , coalesce(u._utilized_hours, 0)     as utilized_hours
    , coalesce(u._non_utilized_hours, 0) as non_utilized_hours
    , {{ cbc_utils.generate_ma_cols(column_names =['revenue', 'utilized_hours', 'non_utilized_hours'],
                        date_field='dlcj.date_at',
                        dimensions=['dlcj.location_id'],
                        ma_days=[7],
                        grain='d',
                        add_coalesce=true) }}

from date_location_cross_join as dlcj
left join revenue as r on dlcj.date_at = r.date_at and dlcj.location_id = r.location_id
left join fct_dvm_utilization as u on dlcj.date_at = u.date_at and dlcj.location_id = u.location_id

```
#### Important Note: Missing Columns and Moving Averages

In a scenario where a value is missing for a given row, the results of a chart will appear wrong. In the example above, if the date/location cross join was not present, on days where a location is not open, no row will exist and a MA for that day will not display. Cross joining with a date/location table is a common practice to ensure that all dates are present in the dataset.

### `generate_pop_columns` - Generate Period Over Period Columns
This macro is used to generate a series of period-over-period columns.

Example 

```sql
select
    r.date_at
    , r.location_id
    , r.location_name
    , r.network_name
    , coalesce(r.total_revenue, 0)                   as total_revenue
    , coalesce(r.discount_sum, 0)                    as discounted_revenue
    , coalesce(r._medical_revenue, 0)                as medical_revenue
    , coalesce(r._service_revenue, 0)                as service_revenue
    , coalesce(r._first_medical_revenue, 0)          as first_medical_revenue
    , coalesce(r._first_service_revenue, 0)          as first_service_revenue
    , coalesce(r.order_count, 0)                     as order_count
    , coalesce(a.medical_appointment_count, 0)       as medical_appointment_count
    , coalesce(a.service_appointment_count, 0)       as service_appointment_count
    , coalesce(a.first_medical_appointment_count, 0) as first_medical_appointment_count
    , coalesce(a.medical_animal_count, 0)            as medical_animal_count
    , coalesce(a.service_animal_count, 0)            as service_animal_count
    , coalesce(r.order_with_appointments_count, 0)   as order_with_appointments_count

    , {{ cbc_utils.generate_pop_columns(column_names = ['total_revenue', 'order_count', 'medical_revenue', 'service_revenue', 'first_medical_revenue',
                                            'first_service_revenue', 'medical_appointment_count', 'service_appointment_count',
                                            'first_medical_appointment_count', 'service_animal_count'],
                            date_field= 'r.date_at',
                            dimensions = ['r.location_id'],
                            look_back_values = [28, 90, 180, 365],
                            grain = 'd') }}
from revenue as r
left join appts as a
    on r.date_at = a.date_at and r.location_id = a.location_id
order by 1 desc, 2

```