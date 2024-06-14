# CBC Utils

## Usage
Add the following to the packages.yml file in your dbt project.
```
  - git: "https://github.com/Cold-Bore-Capital/dbt-cbc-utils.git"
    revision: <get latest release>
```

## Macros

### `generate_ma_columns` - Generate Moving Average Columns

This macro is designed to create moving average columns over a given date column and dimensions. The advantage to setting a moving average in the model vs as a table calculation is that the model has access to data outside the range of the final chart. When setting an MA in a table function, the first few rows will be averaging over fewer than the desired number of days. This can lead to wild swings in the line. By setting the MA in the model, the MA will be calculated using data that is filtered out of the final chart, providing a more accurate representation of the data.

This approach is only useful for highly aggregated models without a large number of dimension, as shown in the examples. 

#### Arguments
- `column_names` (required): A list of column names to generate moving averages for.
- `date_field` (required): The date field to generate the moving average over.
- `dimensions` (required): A list of dimensions to partition the moving average over. 
- `ma_windows` (required): A list of integers representing the number of days/weeks/months/years to calculate the moving average over. For example, [7, 14, 28] will generate columns for 7, 14, and 28 day moving averages if the grain is set to `d` (and each row represents one day).
- `grain` (optional): The grain of the moving average. Default is 'd' for daily.
- `add_coalesce` (optional): If set to true, a coalesce function will be wrapped around the value in the moving average. Default is true. Example with `avg(coalesce(total_revenue,0)) over (partition by location_id order by date_at rows between 6 preceding and current row) as total_revenue_7d_ma` and without `avg(total_revenue) over (partition by location_id order by date_at rows between 6 preceding and current row) as total_revenue_7d_ma`
- `comma_at_end` (optional): If set to `true`, a comma will be added at the end of the generated column. If `false`, the comma will be placed at the start of the row. Default is `true`.

*Note:* A column will be generated for every combination of `column_names` and `ma_windows` value. Adding many dimensions and columns can result in a large number of columns being generated.

#### Example Usage
```sql
select
    date_at,
    location_id,
    location_name,
    network_name,
    coalesce(revenue_total,0)        as revenue,
    coalesce(_utilized_hours, 0)     as utilized_hours,
    coalesce(_non_utilized_hours, 0) as non_utilized_hours,
    {{ cbc_utils.generate_ma_columns(column_names =['revenue', 'utilized_hours', 'non_utilized_hours'],
                        date_field='date_at',
                        dimensions=['location_id'],
                        ma_windows=[7], -- You could put 7, 14, 28 and columns will generate for each.
                        grain='d', -- This is for display purposes. The column name will be set using this value.
                        add_coalesce=true) }}
from revenue
```
#### Example output 
```sql
avg(coalesce(total_revenue,0)) over (partition by location_id order by date_at rows between 6 preceding and current row) as total_revenue_7d_ma,
avg(coalesce(medical_revenue,0)) over (partition by location_id order by date_at rows between 6 preceding and current row) as medical_revenue_7d_ma,
avg(coalesce(service_revenue,0)) over (partition by location_id order by date_at rows between 6 preceding and current row) as service_revenue_7d_ma,
```

#### Important Note: Missing Columns and Moving Averages

In a scenario where a value is missing for a given row, the results of a chart will appear wrong. In the example above, if the date/location cross join was not present, on days where a location is not open, no row will exist and a MA for that day will not display. Cross joining with a date/location table is a common practice to ensure that all dates are present in the dataset.

### `generate_pop_columns` - Generate Period Over Period Columns
This macro is used to generate a series of period-over-period columns.

#### Arguments
- `column_names` (required): A list of column names to generate period-over-period columns for.
- `date_field` (required): The date field to generate the period-over-period columns over.
- `dimensions` (required): A list of dimensions to partition the period-over-period columns over.
- `look_back_values` (required): A list of integers representing the number of days to look back for each period-over-period column.
- `grain` (optional): The grain of the period-over-period columns. Default is 'd' for daily.
- `comma_at_end` (optional): If set to `true`, a comma will be added at the end of the generated column. If `false`, the comma will be placed at the start of the row. Default is `true`.

#### Example Usage

```sql
select
    date_at
    , location_id
    , location_name
    , network_name
    , coalesce(total_revenue, 0)    as total_revenue
    , coalesce(_medical_revenue, 0) as medical_revenue
    , coalesce(_service_revenue, 0) as service_revenue

    , {{ cbc_utils.generate_pop_columns(column_names = ['total_revenue', 'medical_revenue', 'service_revenue'],
                                        date_field= 'date_at',
                                        dimensions = ['location_id'],
                                        look_back_values = [28, 90, 180, 365],
                                        grain = 'd', -- This is for display purposes. The column name will be set using this value.
                                        comma_at_end=false) }}
from revenue

```
*Note:* A sql column will be generated for every combination of `column_names` and `look_back_values`. Adding many dimension & columns can result in a large number of columns being generated.

#### Example output 

```sql
, coalesce(lag(total_revenue, 28) over (partition by r.location_id order by r.date_at), 0) as total_revenue_28d_pop
, coalesce(lag(total_revenue, 90) over (partition by r.location_id order by r.date_at), 0) as total_revenue_90d_pop
, coalesce(lag(total_revenue, 180) over (partition by r.location_id order by r.date_at), 0) as total_revenue_180d_pop
, coalesce(lag(total_revenue, 365) over (partition by r.location_id order by r.date_at), 0) as total_revenue_365d_pop
<--... continues for each column ...-->
```