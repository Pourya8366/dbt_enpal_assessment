with
    source as (select * from {{ source('postgres_public','users') }}),

    renamed as (
        select
            id as user_id,
            concat('User_', lpad(id::text, 3, '0')) as user_name,
            concat('user_', lpad(id::text, 3, '0'), '@company.local') as user_email
        from source
    )

select *
from renamed
