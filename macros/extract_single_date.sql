
{% macro extract_single_date(og_date)%}

    if({{og_date}} like {{og_date}}) then
        return 'wahoo';
    end if;

{% endmacro %}