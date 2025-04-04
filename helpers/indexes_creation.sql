CREATE INDEX IF NOT EXISTS idx_fire_incidents_date_id ON incidents.gold.fact_fire_incidents(date_id);
CREATE INDEX IF NOT EXISTS idx_fire_incidents_battalion ON incidents.gold.fact_fire_incidents(battalion);
CREATE INDEX IF NOT EXISTS idx_fire_incidents_battalion ON incidents.gold.fact_fire_incidents(supervisor_district);

CREATE INDEX IF NOT EXISTS idx_dim_date_date_id ON incidents.gold.dim_date(date_id);




