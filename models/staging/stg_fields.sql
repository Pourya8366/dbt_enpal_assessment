with
    source as (select * from {{ source('postgres_public','fields') }}),

    renamed as (
        select
            id,
            field_key,
            name,
            field_value_options
        from source
    )

select *
from renamed