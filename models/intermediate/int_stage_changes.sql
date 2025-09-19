{{
    config(
        materialized="incremental",
        unique_key="change_time",
        on_schema_change="sync_all_columns",
        sort="change_time",
    )
}}

with

deal_stage_changes as (
    select
        deal_id,
        change_time,
        new_value as stage_id
    from {{ ref('stg_deal_changes') }}
    where changed_field_key = 'stage_id'
    {% if is_incremental() %}

        and
            change_time
            >= (select max(change_time) - INTERVAL '2 day' from {{ this }})

    {% endif %}
),

stages as (
    select *
    from {{ ref('stg_stages') }}
),

stage_changes_enriched as (
    select
        deal_stage_changes.deal_id,
        deal_stage_changes.change_time,
        deal_stage_changes.stage_id,
        stages.stage_name
    from deal_stage_changes
    left join stages on cast(deal_stage_changes.stage_id as int) = stages.stage_id
)

select * from stage_changes_enriched