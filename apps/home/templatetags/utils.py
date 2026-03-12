from django import template

register = template.Library()


@register.filter
def get_item(dictionary, key):
    """Safe dictionary access for templates."""
    if isinstance(dictionary, dict):
        return dictionary.get(key, "")
    return ""


@register.filter
def formato_moneda(value):
    """
    Formatea un numero como moneda colombiana con separador de miles (punto).
    {{ 1234567|formato_moneda }} => "$1.234.567"
    """
    try:
        n = float(value)
    except (TypeError, ValueError):
        return "$0"
    formatted = f"{n:,.0f}".replace(",", ".")
    return f"${formatted}"


@register.filter
def formato_numero(value):
    """
    Formatea un numero con separador de miles (punto).
    {{ 1234567|formato_numero }} => "1.234.567"
    """
    try:
        n = float(value)
    except (TypeError, ValueError):
        return "0"
    return f"{n:,.0f}".replace(",", ".")
