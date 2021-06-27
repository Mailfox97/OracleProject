with temp as
(
select
    date_info
   ,datetime_info
   ,new_cases
   ,new_deaths
   ,dense_rank() over
        (
           partition by  date_info
           order by  datetime_info desc
        ) as latest
from covid_history
where
    country = '{country}'
    and datetime_info <= (
                           select max(datetime_info)
                           from covid_history
                           where country = '{country}'
                        )
   and datetime_info > (
                        select trunc(max(datetime_info)-6)
                        from covid_history
                        where country = '{country}'
                       )
)
select
        date_info
        ,new_cases
        ,new_deaths
from
        temp
where temp.latest = 1