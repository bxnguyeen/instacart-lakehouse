{% macro validate_gold_catalog(include_gold=true) %}
    {% if execute %}
        {% set version = var('catalog_version') %}

        {# Validate the Silver catalog before building or reusing Gold. #}
        {{ validate_silver_catalog() }}

        {% if include_gold %}
            {% set expected = ref('dim_products') %}
            {% set relation = adapter.get_relation(
                database=expected.database,
                schema=expected.schema,
                identifier=expected.identifier
            ) %}

            {% if relation is none %}
                {{ exceptions.raise_compiler_error(
                    'Gold catalog table is missing: dim_products'
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
                    'Gold catalog version validation failed: dim_products'
                ) }}
            {% endif %}

            {% set count_query %}
                select count(*) from {{ ref('stg_products') }}
            {% endset %}
            {% set source_count = run_query(count_query) %}
            {% if total != source_count.rows[0][0] %}
                {{ exceptions.raise_compiler_error(
                    'Gold catalog row count validation failed: dim_products'
                ) }}
            {% endif %}

            {{ log(
                'GOLD_CATALOG_VALIDATED model=dim_products'
                ~ ' rows=' ~ total
                ~ ' version=' ~ version,
                info=True
            ) }}
        {% endif %}
    {% endif %}
{% endmacro %}
