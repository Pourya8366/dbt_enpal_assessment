with
    source as (select * from {{ source('postgres_public','users') }}),

    renamed as (
        select
            {{ dbt_utils.generate_surrogate_key(['id','name','email']) }} as pk
        from source
    )

select *
from renamed
