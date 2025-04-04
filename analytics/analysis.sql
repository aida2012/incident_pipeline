
--Number of incidents per year and district
SELECT 
    EXTRACT(YEAR FROM incident_date) AS year,
    supervisor_district,
    COUNT(*) AS total_incidents
FROM incidents.gold.fact_fire_incidents
GROUP BY 1, 2
ORDER BY 1, 2;


--Days between incidents per district, for the last 7 days
SELECT
    supervisor_district,
    incident_date,
    LAG(incident_date) OVER (PARTITION BY supervisor_district ORDER BY incident_date) AS previous_incident_date,
    COALESCE(
        EXTRACT(DAY FROM (incident_date - LAG(incident_date) OVER (
            PARTITION BY supervisor_district
            ORDER BY incident_date
        ))),
        0
    ) AS days_since_last_incident
FROM gold.fact_fire_incidents where
incident_date >= CURRENT_DATE - INTERVAL '7 days'