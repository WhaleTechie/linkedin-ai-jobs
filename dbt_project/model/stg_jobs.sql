select
  lower(title) as job_title,
  lower(snippet) as snippet,
  link,
  date_crawled
from raw.jobs
