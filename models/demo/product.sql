{{
    config(
        materialized = 'incremental',
        unique_key = 'PRODUCT_ID',
        incremental_strategy = 'delete+insert'
    )
}}

with product_src as (
    select
        PRODUCT_ID,
        PRODUCT_NAME,
        PRODUCT_PRICE,
        CREATED_AT,
        CURRENT_TIMESTAMP() AS INSERT_DTS  -- Fixed column name & added parens
    
    from {{ source('product', 'PRODUCT_SRC') }}

    {% if is_incremental() %} -- Fixed function name
        where CREATED_AT > (select max(INSERT_DTS) from {{ this }})
    {% endif %}
)

select * from product
