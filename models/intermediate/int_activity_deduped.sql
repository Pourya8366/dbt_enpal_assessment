with
    activity as (select * from {{ ref('stg_activity') }}),

    columns_hash as (
        select
            *,
            {{ dbt_utils.generate_surrogate_key(['activity_id','activity_type','assigned_to_user','deal_id','is_done', 'due_to_time']) }} as pk
        from activity
    ),

    ranked as (
        select
            *,
            row_number() over (partition by pk) as row_num
        from columns_hash
    ),

    deduped as (
        select
            pk,
            activity_id,
            activity_type,
            assigned_to_user,
            deal_id,
            is_done,
            due_to_time
        from ranked
        where row_num = 1
    )

select *
from deduped
