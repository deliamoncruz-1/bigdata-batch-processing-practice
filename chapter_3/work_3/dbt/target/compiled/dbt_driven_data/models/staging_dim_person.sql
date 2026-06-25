

WITH source_data AS(
    SELECT
        unique_id,
        person_name,
        user_name,
        email,
        phone_number,
        birth_date,
        personal_number
        FROM "airflow"."driven_raw"."raw_batch_data"
)
  SELECT *
    FROM source_data