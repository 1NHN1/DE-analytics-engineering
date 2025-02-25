{{
    config(
        materialized='view'
    )
}}



WITH taxi_revenue_quarterly AS (
    SELECT 
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
        -- Aquí asumimos que total_amount representa el total de ingresos por viaje
        SUM(total_amount) AS quarterly_revenue,
        service_type
    FROM {{ ref('fact_trips') }}
    GROUP BY 
        year, quarter, service_type
),

taxi_revenue_yoy_growth AS (
    SELECT 
        t1.year,
        t1.quarter,
        t1.service_type,
        t1.quarterly_revenue AS current_quarter_revenue,
        t2.quarterly_revenue AS previous_quarter_revenue,
        CASE 
            WHEN t2.quarterly_revenue IS NULL THEN NULL  -- Evitar dividir por NULL
            ELSE (t1.quarterly_revenue - t2.quarterly_revenue) / t2.quarterly_revenue * 100
        END AS yoy_growth_percentage
    FROM 
        taxi_revenue_quarterly t1
    LEFT JOIN 
        taxi_revenue_quarterly t2
    ON 
        t1.service_type = t2.service_type
        AND t1.quarter = t2.quarter
        AND t1.year = t2.year + 1  -- Comparar con el mismo trimestre del año anterior
)

-- Insertar o seleccionar los resultados finales con YoY Growth
SELECT 
    year,
    quarter,
    service_type,
    current_quarter_revenue,
    previous_quarter_revenue,
    yoy_growth_percentage
FROM 
    taxi_revenue_yoy_growth
ORDER BY 
    year, quarter, service_type
