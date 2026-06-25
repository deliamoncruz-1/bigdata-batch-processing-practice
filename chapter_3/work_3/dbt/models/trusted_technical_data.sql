{{ config(
    materialized='table',
    schema='trusted',
    alias='technical_data',
    tags=['trusted']
) }}

WITH source_data AS (
    SELECT
        fnu.unique_id,
        da.address,
        da.mac_address,
        da.ip_address,
        fnu.download_speed,
        fnu.upload_speed,
        round(fnu.session_duration/60, 1) as min_session_duration,
        case
            when fnu.download_speed < 50
              or fnu.upload_speed < 30
              or (fnu.session_duration / 60) < 1
            then true
            else false
        end as technical_issue
    FROM {{ ref('staging_fact_network_usage') }} fnu
    JOIN {{ ref('staging_dim_address') }} da
        ON fnu.unique_id = da.unique_id
)

SELECT *
FROM source_data