{{
    config(
        tags = ["daily"],
        materialized = "view"
    )
}}

select * from {{ ref('user_analytics')}}