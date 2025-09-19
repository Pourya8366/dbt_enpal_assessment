with
    source as (select * from {{ source('postgres_public','activity_types') }}),

    renamed as (
        select
            id as activity_type_id,
            name as activity_name,
            case active
                when 'Yes' then true
                when 'No' then false
            end as is_active,
            type as activity_type
        from source
    )

select *
from renamed
