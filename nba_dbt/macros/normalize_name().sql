{% macro normalize_name(col) %}
    lower(
        trim(
            regexp_replace(
                translate({{ col }}, 
                    'áàäâÁÀÄÂéèëêÉÈËÊíìïîÍÌÏÎóòöôÓÒÖÔúùüûÚÙÜÛñÑçÇćĆĄąĘęŁłŃńÓóŚśŹźŻżČčņģŠíjū', 
                    'aaaaAAAAeeeeEEEEiiiiIIIIooooOOOOuuuuUUUUnNcCcCAaEeLlNnOoSsZzZzCcngSiju'
                    ),'[\.\'-]', ' '
            )
        )
)
{% endmacro %}
