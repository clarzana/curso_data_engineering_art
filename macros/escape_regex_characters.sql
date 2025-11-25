-- Carácteres a escapar:
-- ^$.|?*+()[]{}\
{%- macro escape_regex_characters(my_value) -%}
    -- {{ my_value|replace('\\', '\\\\')|replace('$', '\$')|replace('.', '\.')|replace('|', '\|')|replace('?', '\?')|replace('*', '\*')|replace('+', '\+')|replace('(', '\(')|replace(')', '\)')|replace('[', '\[')|replace(']', '\]')|replace('{', '\{')|replace('}', '\}')|replace('\\', '\\\\') }}
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace({{ my_value }},
    '\\', '\\\\'),
    '$', '\\$'),
    '.', '\\.'),
    '|', '\\|'),
    '?', '\\?'),
    '*', '\\*'),
    '+', '\\+'),
    '(', '\\('),
    ')', '\\)'),
    '[', '\\['),
    ']', '\\]'),
    '{', '\\{'),
    '}', '\\}')
{%- endmacro -%}