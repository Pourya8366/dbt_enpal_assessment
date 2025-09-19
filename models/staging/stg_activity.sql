with
    source as (select * from {{ source('postgres_public','activity') }}),

    renamed as (
        select
            activity_id,
            type as activity_type,
            assigned_to_user,
            deal_id,
            done as is_done,
            due_to as due_to_time
        from source
    )

select *
from renamed
