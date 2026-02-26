from django import template

register = template.Library()


@register.filter
def get_item(dictionary, key):
    """Safe dictionary access for templates."""
    if isinstance(dictionary, dict):
        return dictionary.get(key, "")
    return ""
