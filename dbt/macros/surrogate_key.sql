{% macro surrogate_key(fields) -%}
  lower(md5(
    concat_ws('||',
      {%- for f in fields -%}
        coalesce(cast({{ f }} as varchar), '')
        {%- if not loop.last -%}, {% endif -%}
      {%- endfor -%}
    )
  ))
{%- endmacro %}
