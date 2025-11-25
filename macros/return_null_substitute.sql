
{%- macro return_null_substitute(my_value, target_table) %}
    {%- if target_table=='art_schools' %}
        {%- set target_table_null = var("art_school_null") %}
    {%- elif target_table=='artists' %}
        {% set target_table_null = var("artist_null") %}
    {%- elif target_table=='places' %}
        {% set target_table_null = var("place_null") %}
    {%- elif target_table=='movements' %}
        {% set target_table_null = var("movement_null") %}
    {%- elif target_table=='styles' %}
        {% set target_table_null = var("style_null") %}
    {%- elif target_table=='occupations' %}
        {% set target_table_null = var("occupation_null") %}
    {%- elif target_table=='media' %}
        {% set target_table_null = var("medium_null") %}
    {%- elif target_table=='genres' %}
        {% set target_table_null = var("genre_null") %}
    {%- elif target_table=='genders' %}
        {% set target_table_null = var("gender_null") %}
    {%- elif target_table=='exhibitions_journals' %}
        {% set target_table_null = var("exhibition_null") %}
    {% endif %}
    ifnull({{ my_value }}, {{ target_table_null }})
{% endmacro %}