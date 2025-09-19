{{
    config(
        materialized="incremental",
        unique_key="month",
        on_schema_change="sync_all_columns",
        sort="month",
    )
}}

with
stage_changes as (
    select *
    from {{ ref('int_stage_changes') }}
    {% if is_incremental() %}

        where
            date_trunc('month', change_time)
            >= (select max(month) - interval '1 month' from {{ this }})

    {% endif %}
),

sales_call_activity as (
    select
        due_to_time,
        deal_id,
        activity_type
    from {{ ref('stg_activity') }}
    where activity_type in ('meeting', 'sc_2') and is_done = true
),

activity_type_sale_call as (
    select *
    from {{ ref('stg_activity_types') }}
    where activity_type in ('meeting', 'sc_2')
),

-- Stage changes to generate main funnel steps
stage_counts as (
    select
        date_trunc('month', change_time) as month,
        stage_name as kpi_name,
        stage_id as funnel_step,
        count(distinct deal_id) as deals_count
    from stage_changes
    group by month, kpi_name, funnel_step
),

-- Sales calls to generate substeps within the funnel
sales_calls as (
    select
        date_trunc('month', sales_call_activity.due_to_time) as month,
        activity_type_sale_call.activity_name as kpi_name,
        case activity_type_sale_call.activity_type
            when 'meeting' then '2.1'
            when 'sc_2'   then '3.1'
        end as funnel_step,
        count(distinct sales_call_activity.deal_id) as deals_count
    from sales_call_activity
    left join activity_type_sale_call
      on sales_call_activity.activity_type = activity_type_sale_call.activity_type
    group by month, kpi_name, funnel_step
),

funnel_unioned as (
    select
        month,
        kpi_name,
        funnel_step,
        deals_count
    from stage_counts
    union all
    select
        month,
        kpi_name,
        funnel_step,
        deals_count
    from sales_calls
),

final as (
    select
        month,
        kpi_name,
        funnel_step,
        deals_count
    from funnel_unioned
)

select * from final