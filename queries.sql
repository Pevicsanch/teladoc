-- Creamos la primera tabla temporal (bg_last_30) para almacenar los cálculos relacionados a los últimos 30 días
-- de cada usuario. Estos cálculos incluyen el primer chequeo de glucosa en sangre, la cantidad total de chequeos,
-- el valor medio, la cantidad promedio de chequeos por día y las lecturas hipoglucémicas.
WITH bg_last_30 AS (
    SELECT 
        user_id, 
        -- Primera fecha de chequeo de glucosa en sangre para cada usuario
        MIN(bg_timestamp) OVER (PARTITION BY user_id) AS first_bg_check,
        -- Cantidad de chequeos en los últimos 30 días
        COUNT(CAST(bg_value AS FLOAT)) AS checks_last_30_days,
        -- Valor promedio de glucosa en sangre en los últimos 30 días
        AVG(CAST(bg_value AS FLOAT)) AS mean_value_last_30_days,
        -- Cantidad promedio de chequeos de glucosa en sangre por día en los últimos 30 días
        COUNT(CAST(bg_value AS FLOAT))/30.0 AS avg_checks_per_day_last_30_days,
        -- Cantidad de lecturas hipoglucémicas (bg_value < 54) en los últimos 30 días
        COUNT(CASE WHEN CAST(bg_value AS FLOAT) < 54 THEN 1 ELSE NULL END) AS hypo_readings_last_30_days
    FROM 
        -- Usamos la tabla blood_glucose_checks y filtramos solo las filas que tengan un timestamp
        -- dentro de los últimos 30 días.
        blood_glucose_checks 
    WHERE 
        bg_timestamp > CURRENT_DATE - INTERVAL '30 days'
), 
-- Creamos la segunda tabla temporal (hypo_readings) para almacenar la cantidad total de lecturas hipoglucémicas
-- de cada usuario a lo largo de su vida.
hypo_readings AS (
    SELECT 
        user_id, 
        -- Cantidad de lecturas hipoglucémicas (bg_value < 54) a lo largo de la vida
        COUNT(CASE WHEN CAST(bg_value AS FLOAT) < 54 THEN 1 ELSE NULL END) AS hypo_readings_lifetime
    FROM 
        -- Usamos la tabla blood_glucose_checks sin ningún filtro de fecha
        blood_glucose_checks 
    GROUP BY 
        user_id
)
-- Finalmente, seleccionamos la información que queremos en nuestra salida final.
SELECT 
    m.user_id,
    bg.first_bg_check,
    bg.checks_last_30_days,
    bg.mean_value_last_30_days,
    bg.avg_checks_per_day_last_30_days,
    hr.hypo_readings_lifetime,
    bg.hypo_readings_last_30_days
FROM 
    -- Comenzamos con la tabla miembros y la filtramos para incluir solo los usuarios masculinos y mayores de 18 años.
    members m
-- Unimos las tablas temporales creadas previamente con la tabla members usando el campo user_id
JOIN 
    bg_last_30 bg ON m.user_id = bg.user_id
JOIN 
    hypo_readings hr ON m.user_id = hr.user_id
WHERE 
    m.gender = 'M' 
    AND (CURRENT_DATE - m.birth_date)/365.25 >= 18
