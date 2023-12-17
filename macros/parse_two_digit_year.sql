{% macro parse_two_digit_year(year_str) %}
    -- This macro converts a two-digit year string to a four-digit year integer.
    -- It assumes a cutoff of 69, so that 69-99 are interpreted as 1969-1999 and 00-68 are interpreted as 2000-2068.
    case
        when {{ year_str }}::int < 69 then {{ year_str }}::int + 2000
        else {{ year_str }}::int + 1900
    end
{% endmacro %}
