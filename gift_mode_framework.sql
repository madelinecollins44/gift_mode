--track gift mode impressions: 
----visits with a gift mode impression
----total impressions 
----visit level click rate
----event level click rate

	with get_recmods_events as (
  select
		date(_partitiontime) as _date
		, visit_id
		, sequence_number
		, beacon.event_name as event_name
		, (select value from unnest(beacon.properties.key_value) where key = 'module_placement') as module_placement

	from
		`etsy-visit-pipe-prod.canonical.visit_id_beacons`
	where 
    date(_partitiontime) >= current_date-2
	  and ((beacon.event_name = 'recommendations_module_delivered' and (select value from unnest(beacon.properties.key_value) where key = 'module_placement') in ('lp_suggested_personas_related','homescreen_gift_mode_personas')) --related personas module on listing page, web AND app home popular personas module delivered, boe
        or beacon.event_name in 
        ------various ingresses + banners 
            ('gift_mode_shop_by_occasions_module_seen' --shop by occasion module on homepage, web
              , 'gm_gift_page_ingress_loaded' -- gift mode promo banner on gift category page, web
              , 'search_gift_mode_banner_seen' -- gift mode promo banner on search page, web
              , 'gm_hp_banner_loaded_seen' --homepage banner, web
              , 'search_gift_mode_banner_seen'-- bottom of search page for gift queries, web
              , 'gift_mode_introduction_modal_shown' --Gift Mode introduction overlay shown on homescreen, boe
              , 
        ------core visits 
              , 'gift_mode_home' --gift mode home, boe + web
              , 'gift_mode_persona'-- gift mode personas, boe + web 
              , 'gift_mode_occasions_page'-- gift mode occasions, web
              , 'gift_mode_browse_all_personas' -- see all personas, web
              , 'gift_mode_see_all_personas' -- see all personas, boe
              , 'gift_mode_results' -- gift mode quiz results, web
              , 'gift_mode_quiz_results' -- gift mode quiz results, boe
              -- boe search
            ))
    )
    

