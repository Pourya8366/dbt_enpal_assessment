with
    source as (select * from {{ source('postgres_public','activity_types') }}),

    renamed as (
        select
            id,
            name as activity_name,
            active as is_active,
            type as activity_type
        from source
    )

select *
from renamed