{% macro validate_silver_catalog() %}
    {% if execute %}
        {% set version = var('catalog_version') %}

        {% for model_name in [
            'stg_aisles',
            'stg_departments',
            'stg_products'
        ] %}
            {% set expected = ref(model_name) %}
            {% set relation = adapter.get_relation(
                database=expected.database,
                schema=expected.schema,
                identifier=expected.identifier
            ) %}

            {% if relation is none %}
                {{ exceptions.raise_compiler_error(
                    'Silver catalog table is missing: ' ~ model_name
                ) }}
            {% endif %}

            {% set query %}
                select
                    count(*) as total_rows,
                    count(case
                        when _catalog_version = '{{ version }}'
                        then 1
                    end) as matching_rows
                from {{ relation }}
            {% endset %}

            {% set result = run_query(query) %}
            {% set total = result.rows[0][0] %}
            {% set matching = result.rows[0][1] %}

            {% if total == 0 or total != matching %}
                {{ exceptions.raise_compiler_error(
                    'Silver catalog version validation failed: '
                    ~ model_name
                ) }}
            {% endif %}

            {{ log(
                'SILVER_CATALOG_VALIDATED model=' ~ model_name
                ~ ' rows=' ~ total
                ~ ' version=' ~ version,
                info=True
            ) }}
        {% endfor %}
    {% endif %}
{% endmacro %}