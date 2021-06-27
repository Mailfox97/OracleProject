select
    datetime_info
   ,total_cases
   ,recovered_cases
   ,total_deaths
from covid_history
where
    country = '{country}'
    and datetime_info = (
                           select max(datetime_info)
                           from covid_history
                           where country = '{country}'
                        )