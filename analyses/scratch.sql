
select
    count(*) as n_records

from jhra_staging.stg_jrdb__kyi
where `体型_全体` IS NULL
