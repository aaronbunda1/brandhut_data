
{% macro get_brand_from_sku(sku_column) %}
    case  
        when {{ sku_column }} ilike '%shield%' then 'Onanoff 2'
        when {{ sku_column }} ilike any ('%roku%','%sunny%','%siri%') then '73&Sunny'
        when {{ sku_column }} ilike any ('%-TH-%') then 'Tiny Tree Houses'
        when {{ sku_column }} ilike '%alora%' then 'Alora'
        when {{ sku_column }} ilike '%ZED%' or {{ sku_column }} in (
            'ZEAPM03/00',
            'ZESC08W') 
            or {{ sku_column }} ilike 'ZE%' then 'ZENS'
        when {{sku_column}} ilike '%zens%' then 'ZENS'
        when {{ sku_column }} ilike any ('%storyph%','%ss-%') then 'Onanoff 2'
        when {{ sku_column }} ilike any ('%fokus%') then 'Fokus'
        when {{ sku_column }} ilike '%SPOT%' then 'SPOT'
        when {{ sku_column }} ilike '%QI%' then 'Qisten'
        when {{ sku_column }} ilike any ('CL%','%cellini%') then 'Cellini'
        when {{ sku_column }} ilike any ('%pop-fun%','%BP-%','BP-%','%-ON-%','%ON-%','%buddy%','%onanoff%','%onanoff%','%on-%','%bp-%','%play%','%fun%','%school%','%onanff%','%Onaonff%','%cosmo%','%explore%','%buddy%','%phones%') then 'ONANOFF'
        when {{ sku_column }} ilike any ('%T-%','%tiny%','%tree%') then 'Tiny Tree Houses'
        when {{ sku_column }} IN ('1005-30oz-SB') then 'Legacy'
    end
{% endmacro %}
