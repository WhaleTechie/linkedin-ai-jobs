select
  date_trunc('week', date_crawled) as week,
  sum(pytorch) as pytorch_mentions,
  sum(tensorflow) as tensorflow_mentions,
  sum(airflow) as airflow_mentions,
  sum(dbt) as dbt_mentions,
  sum(aws) as aws_mentions,
  sum(docker) as docker_mentions,
  sum(kubernetes) as kubernetes_mentions,
  sum(sql) as sql_mentions,
  sum(python) as python_mentions
from {{ ref('job_skills') }}
group by 1
order by 1 desc
