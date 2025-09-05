{% test email_format(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and not regexp_full_match({{ column_name }}, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
{% endtest %}
