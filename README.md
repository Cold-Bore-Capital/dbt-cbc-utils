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
- `grain` (optional): The grain of the moving average. Default is 'd' for daily. This is used to set the display label. 
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
                        ma_windows=[7], 
                        grain='d', 
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

In a scenario where a value is missing for a given row, the results of a chart will appear wrong. In the example above, if the date/location cross join was not present, on days when a location is not open, no row will exist and a MA for that day will not display. Cross joining with a date/location table is a common practice to ensure that all dates are present in the dataset.

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
                                        grain = 'd',
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

Here's the addition you can make to the existing documentation, following the same style:

### `bridge_waterfall` - Generate Bridge Waterfall Chart

This macro creates a model that can be used to create a bridge waterfall type chart that allows for multiple periods and week, month, quarter, and year charts.

#### Arguments
- `period` (required): The time period for the chart. Can be 'week', 'month', 'quarter', or 'year'.
- `date_field` (required): The date field to use for the chart.
- `dimension` (required): The dimension to group the data by.
- `metric` (required): The metric to measure in the chart.
- `model` (optional, however either `model` or `cte` is required): The name of the model to use for the chart.
- `cte` (optional, however either `model` or `cte` is required): If the macro is being used after a CTE, the name of the CTE to use for the chart.
- `filter_dimensions` (optional): A list of dimensions to filter the data by. These can be used as filters only, not as dimensions in the chart.
- `start_date` (optional): The start date for the chart data.

#### Example Usage

##### With a model:
```sql
{{ cbc_utils.bridge_waterfall(
                    period='month',
                    date_field='date_at',
                    dimension='business_unit',
                    metric='billed_total_dollars',
                    model='billed_vs_paid_metrics',
                    start_date='2024-01-01') }}
```

##### With a cte:
```sql
with data_input as (
    select *
    from my data
), 
{{ cbc_utils.bridge_waterfall(
                    period='month',
                    date_field='date_at',
                    dimension='business_unit',
                    metric='billed_total_dollars',
                    cte='billed_vs_paid_metrics',
                    filter_dimensions=['end_market'],
                    start_date='2024-01-01') }}
```

This macro will generate a SQL query that creates a bridge waterfall chart based on the specified parameters. The resulting chart will show how the specified metric changes over time for each category in the given dimension, starting from the provided start date.