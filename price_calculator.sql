WITH date_range AS (
  SELECT
    DATE_TRUNC(CURRENT_DATE(), MONTH)                               AS end_date,
    DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 12 MONTH) AS start_date
),

offices_to_exclude AS (
  SELECT office_id
  FROM data_marts.pg_listings
  WHERE active
  GROUP BY office_id
  HAVING COUNT(DISTINCT listing_id) >= 100
),

hotspots AS (
  SELECT
    h.country_code, h.adm_level_1, h.adm_level_2, h.rank,
    i.locality, i.island
  FROM data_marts.hotspot_composite_scores_6m h
  LEFT JOIN datasets.hotspot_island_map i ON i.island = h.adm_level_2
  WHERE h.rank <= 100
),

country_daily AS (
  SELECT
    country_code,
    date,
    COUNT(DISTINCT listing_id)                                       AS listings,
    SUM(lead_count)                                                  AS leads,
    COUNT(DISTINCT if(account_type = 'elite',listing_id,null))       AS elite_listings,
    SUM(if(account_type = 'elite',lead_count,0))                     AS elite_leads,
    COUNT(DISTINCT IF(feature_status = 'elp', listing_id, NULL))    AS elp_listings,
    SUM(IF(feature_status = 'elp', lead_count, 0))                  AS elp_leads
  FROM datasets.tableau_listing_data l
  LEFT JOIN offices_to_exclude o USING (office_id)
  CROSS JOIN date_range d
  WHERE l.date >= d.start_date
    AND l.date < d.end_date
    AND Category = 'Listing::RealEstateListing'
    AND (rental = FALSE OR rental IS NULL)
    AND account_type LIKE '%elite%'
    AND o.office_id IS NULL
  GROUP BY 1, 2
),

country_dal AS (
  SELECT
    country_code,
    AVG(listings)     AS dal_country,
    SUM(leads)        AS leads_12m_country,
    AVG(elp_listings) AS elp_dal_country,
    SUM(elp_leads)    AS elp_leads_country,
    SUM(elite_leads)  AS elite_leads_country,
    AVG(elite_listings) AS elite_dal_country
  FROM country_daily
  GROUP BY 1
),

base AS (
  SELECT
    l.listing_id, l.date, l.country_code, l.locality, l.adm_level_2,
    l.lead_count, l.account_type, l.price_eur, l.price_on_request,
    l.impressions, l.Category, l.rental,
    CASE WHEN hs.country_code IS NOT NULL THEN l.country_code ELSE 'GLOBAL' END AS country_group,
    hs.adm_level_2 AS district_key
  FROM datasets.tableau_listing_data l
  LEFT JOIN hotspots hs
    ON  l.country_code = hs.country_code
    AND CASE 
      WHEN hs.island IS NOT NULL THEN hs.locality = lower(l.locality)
      WHEN hs.adm_level_2= "Dubai" THEN hs.adm_level_2 = l.adm_level_1
      ELSE hs.adm_level_2 = l.adm_level_2
    END
),

district_map AS (
  SELECT
    country_group,
    district_key,
    IFNULL(district_key, 'Other') AS district_group
  FROM base, date_range d
  WHERE date >= d.start_date
    AND date < d.end_date
    AND Category = 'Listing::RealEstateListing'
    AND (rental = FALSE OR rental IS NULL)
  GROUP BY 1, 2
),

elp_snapshot AS (
  SELECT
    l.country_code, l.adm_level_1, l.adm_level_2, l.locality,
    COUNT(DISTINCT e.listing_id) AS elp_count
  FROM `jamesedition-152413.data_marts.pg_promoted_elite_plus_listings` e
  LEFT JOIN `jamesedition-152413.data_marts.listing_admin_map` l USING (listing_id)
  WHERE e.active
  GROUP BY 1, 2, 3,4
),

district_elp AS (
  SELECT
    CASE WHEN hs.country_code IS NOT NULL THEN es.country_code ELSE 'GLOBAL' END AS country_group,
    IFNULL(hs.adm_level_2, 'Other')                                               AS district_key,
    SUM(es.elp_count)                                                             AS elp_count
  FROM elp_snapshot es
  LEFT JOIN hotspots hs
    ON  es.country_code = hs.country_code
    AND CASE 
      WHEN hs.island IS NOT NULL THEN hs.locality = lower(es.locality)
      WHEN hs.adm_level_2= "Dubai" THEN hs.adm_level_2 = es.adm_level_1
      ELSE hs.adm_level_2 = es.adm_level_2
    END
  GROUP BY 1, 2
),

district_elp_grouped AS (
  SELECT
    dm.country_group,
    dm.district_group,
    SUM(de.elp_count) AS elp_count
  FROM district_elp de
  JOIN district_map dm
    ON  de.country_group = dm.country_group
    AND IFNULL(de.district_key, 'Other') = dm.district_group
  GROUP BY 1, 2
),

district_impressions AS (
  SELECT
    dm.country_group,
    dm.district_group,
    SUM(b.impressions) / 4.0      AS impressions,
    SUM(b.impressions) / 400000.0 AS ideal_slots
  FROM base b
  JOIN district_map dm
    ON  b.country_group = dm.country_group
    AND IFNULL(b.district_key, 'Other') = dm.district_group
  CROSS JOIN date_range d
  WHERE b.date >= d.start_date
    AND b.date < d.end_date
  GROUP BY 1, 2
),

district_agg AS (
  SELECT
    dm.country_group                                                                   AS country_code,
    dm.district_group                                                                  AS state_district,
    APPROX_QUANTILES(
      IF(b.lead_count > 0 AND NOT b.price_on_request AND b.account_type LIKE '%elite%', b.price_eur, NULL),
      100
    )[OFFSET(50)]                                                                      AS median_lead_value,
    MAX(cd.leads_12m_country)   AS leads_12m_country,
    MAX(cd.dal_country)         AS dal_country,
    MAX(cd.elp_leads_country)   AS elp_leads_country,
    MAX(cd.elp_dal_country)     AS elp_dal_country,
    MAX(deg.elp_count)          AS elp_count,
    MAX(di.ideal_slots)         AS ideal_slots,
    MAX(cd.elite_leads_country) AS elite_leads_country,
    MAX(cd.elite_dal_country)  AS elite_dal_country,

    COUNT(DISTINCT IF(
      b.lead_count > 0 AND NOT b.price_on_request AND b.account_type LIKE '%elite%',
      b.listing_id, NULL
    )) AS listings_with_leads,
    SUM(IF(
      b.lead_count > 0 AND NOT b.price_on_request AND b.account_type LIKE '%elite%',
      b.lead_count, 0
    )) AS total_leads
    


  FROM base b
  JOIN district_map dm
    ON  b.country_group = dm.country_group
    AND IFNULL(b.district_key, 'Other') = dm.district_group
  CROSS JOIN date_range d
  LEFT JOIN country_dal cd           ON b.country_code    = cd.country_code
  LEFT JOIN district_elp_grouped deg ON dm.country_group  = deg.country_group AND dm.district_group = deg.district_group
  LEFT JOIN district_impressions di  ON dm.country_group  = di.country_group  AND dm.district_group = di.district_group
  WHERE b.date >= d.start_date
    AND b.date < d.end_date
    AND b.account_type LIKE '%elite%'
  GROUP BY 1, 2
  
),

calc AS (
  SELECT
    total_leads,
    country_code,
    state_district,
    median_lead_value,
    CASE country_code
      WHEN 'PT' THEN 0.03
      WHEN 'FR' THEN 0.03
      WHEN 'ES' THEN 0.03
      WHEN 'US' THEN 0.055
      WHEN 'CH' THEN 0.025
      WHEN 'AE' THEN 0.02
      WHEN 'CA' THEN 0.04
    
      ELSE            0.03
    END                                                                        AS agency_commission_by_country,
    0.03                                                                       AS conversion_to_deal,
    0.10                                                                       AS take_rate_5,
    0.08                                                                       AS take_rate_15,
    0.07                                                                       AS take_rate_30,
    0.06                                                                       AS take_rate_60,
    0.05                                                                       AS take_rate_100,
    0.10                                                                       AS take_rate_elp,
    (elite_leads_country / 4.0) / NULLIF(elite_dal_country, 0)                    AS leads_per_listing,
    (elp_leads_country / 4.0) / NULLIF(elp_dal_country, 0)                    AS lead_per_elp_3m,
    GREATEST(LEAST(COALESCE(elp_count, ideal_slots) / NULLIF(ideal_slots, 0), 1.3), 0.9) AS demand_multiplier
  FROM district_agg
)

SELECT
  *,
  ROUND(median_lead_value * agency_commission_by_country * conversion_to_deal * leads_per_listing * take_rate_5,   2) AS price_per_listing_elite_5,
  ROUND(median_lead_value * agency_commission_by_country * conversion_to_deal * leads_per_listing * take_rate_15,  2) AS price_per_listing_elite_15,
  ROUND(median_lead_value * agency_commission_by_country * conversion_to_deal * leads_per_listing * take_rate_30,  2) AS price_per_listing_elite_30,
  ROUND(median_lead_value * agency_commission_by_country * conversion_to_deal * leads_per_listing * take_rate_60,  2) AS price_per_listing_elite_60,
  ROUND(median_lead_value * agency_commission_by_country * conversion_to_deal * leads_per_listing * take_rate_100, 2) AS price_per_listing_elite_100,
  ROUND(median_lead_value * agency_commission_by_country * conversion_to_deal * lead_per_elp_3m  * take_rate_elp * demand_multiplier, 0) AS price_per_elp_3m
FROM calc
WHERE total_leads/4 >= 80
and country_code in ("PT", "FR", "ES", "GB","GR","IT")
