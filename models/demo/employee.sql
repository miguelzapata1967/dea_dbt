{{
    config(
        materialized = 'table'
    )
}}

with employee_transformed as (
    select
        EMPID as emp_id,
        split_part(NAME, ' ', 1) as emp_firstname,
        split_part(NAME, ' ', 2) as emp_lastname,
        SALARY as emp_salary,
        HIREDATE as emp_hiredate, -- Fixed the comma typo from your previous version
        -- Splitting address and trimming extra spaces
        trim(split_part(ADDRESS, ',', 1)) as emp_streetname,
        trim(split_part(ADDRESS, ',', 2)) as emp_city,
        trim(split_part(ADDRESS, ',', 3)) as emp_country,
        trim(split_part(ADDRESS, ',', 4)) as emp_zipcode
    from {{source('employee', 'EMPLOYEE_RAW') }}--DBT_DB.PUBLIC.EMPLOYEE_RAW
)

select * from employee_transformed