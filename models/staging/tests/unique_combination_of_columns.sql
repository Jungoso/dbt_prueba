select
  {{ combination_of_columns | join(', ') }}
from {{ model }}
group by {{ combination_of_columns | join(', ') }}
having count(*) > 1