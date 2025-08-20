select
  job_title,
  snippet,
  case when snippet like '%pytorch%' then 1 else 0 end as pytorch,
  case when snippet like '%tensorflow%' then 1 else 0 end as tensorflow,
  case when snippet like '%airflow%' then 1 else 0 end as airflow,
  case when snippet like '%dbt%' then 1 else 0 end as dbt,
  case when snippet like '%aws%' then 1 else 0 end as aws,
  case when snippet like '%docker%' then 1 else 0 end as docker,
  case when snippet like '%kubernetes%' then 1 else 0 end as kubernetes,
  case when snippet like '%sql%' then 1 else 0 end as sql,
  case when snippet like '%python%' then 1 else 0 end as python,
  date_crawled
from {{ ref('stg_jobs') }}
