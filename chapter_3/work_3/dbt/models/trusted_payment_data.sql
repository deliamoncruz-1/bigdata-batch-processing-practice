{{ config(
    materialized='table',
    schema='trusted',
    alias='payment_data',
    tags=['trusted']
) }}

WITH source_data AS (
    SELECT
        fnu.unique_id,
        df.clabe,
        fnu.download_speed,
        fnu.upload_speed,
        fnu.session_duration,
        fnu.consumed_traffic,
        ((fnu.download_speed + fnu.upload_speed + 1) / 2 +
        (COALESCE(
            fnu.consumed_traffic / NULLIF(fnu.session_duration, 0),
            0
        ) + 1)) AS payment_amount
    FROM {{ ref('staging_fact_network_usage') }} fnu
    JOIN {{ ref('staging_dim_finance') }} df
        ON fnu.unique_id = df.unique_id
)

SELECT *
FROM source_data