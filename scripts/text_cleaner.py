#!/usr/bin/env python
"""
Utilidad para limpiar texto y caracteres problemáticos para hojas de cálculo
"""
import re
import unicodedata

class TextCleaner:
    """Clase para limpiar texto problemático en hojas de cálculo"""
    
    @staticmethod
    def clean_for_excel(text):
        """
        Limpia texto para que sea compatible con Excel/hojas de cálculo
        
        Args:
            text (str): Texto a limpiar
            
        Returns:
            str: Texto limpio
        """
        if not text or not isinstance(text, str):
            return str(text) if text is not None else ""
        
        # Eliminar caracteres de control (excepto \t, \n, \r)
        text = ''.join(char for char in text if ord(char) >= 32 or char in '\t\n\r')
        
        # Reemplazar caracteres problemáticos específicos
        problematic_chars = {
            '\x00': '',  # NULL
            '\x01': '',  # Start of Heading
            '\x02': '',  # Start of Text
            '\x03': '',  # End of Text
            '\x04': '',  # End of Transmission
            '\x05': '',  # Enquiry
            '\x06': '',  # Acknowledge
            '\x07': '',  # Bell
            '\x08': '',  # Backspace
            '\x0B': '',  # Vertical Tab
            '\x0C': '',  # Form Feed
            '\x0E': '',  # Shift Out
            '\x0F': '',  # Shift In
            '\x10': '',  # Data Link Escape
            '\x11': '',  # Device Control 1
            '\x12': '',  # Device Control 2
            '\x13': '',  # Device Control 3
            '\x14': '',  # Device Control 4
            '\x15': '',  # Negative Acknowledge
            '\x16': '',  # Synchronous Idle
            '\x17': '',  # End of Transmission Block
            '\x18': '',  # Cancel
            '\x19': '',  # End of Medium
            '\x1A': '',  # Substitute
            '\x1B': '',  # Escape
            '\x1C': '',  # File Separator
            '\x1D': '',  # Group Separator
            '\x1E': '',  # Record Separator
            '\x1F': '',  # Unit Separator
        }
        
        for char, replacement in problematic_chars.items():
            text = text.replace(char, replacement)
        
        # Normalizar caracteres Unicode
        text = unicodedata.normalize('NFKD', text)
        
        # Limpiar espacios múltiples
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    @staticmethod
    def clean_batch(data_list):
        """
        Limpia una lista de textos
        
        Args:
            data_list (list): Lista de textos a limpiar
            
        Returns:
            list: Lista de textos limpios
        """
        return [TextCleaner.clean_for_excel(item) for item in data_list]
    
    @staticmethod
    def clean_dict(data_dict):
        """
        Limpia un diccionario de datos
        
        Args:
            data_dict (dict): Diccionario con datos a limpiar
            
        Returns:
            dict: Diccionario con datos limpios
        """
        cleaned = {}
        for key, value in data_dict.items():
            if isinstance(value, str):
                cleaned[key] = TextCleaner.clean_for_excel(value)
            elif isinstance(value, list):
                cleaned[key] = TextCleaner.clean_batch(value)
            elif isinstance(value, dict):
                cleaned[key] = TextCleaner.clean_dict(value)
            else:
                cleaned[key] = value
        return cleaned

def test_cleaner():
    """Función de prueba para el limpiador de texto"""
    
    # Texto de ejemplo con caracteres problemáticos
    problematic_text = """
    Nombre completo: Lucas Molina
    Número de cédula / NIT: NA
    Nombre del negocio: Transolu
    Tipo de cliente: NA
    Número de celular: 3146517241 / 3146515964
    Correo electrónico: contabilidad@transolusas.com
    Dirección: Carrera 83 # 33 - 09 int 301 / Calle 85 No. 48-01 Piso 8 Oficina 841 Bloque 31 Torre A
    Ciudad de residencia: Medellín
    Objeto de consulta: Nos complace presentar nuestra compañía...
    """
    
    print("=== LIMPIADOR DE TEXTO PARA EXCEL ===")
    print("\nTexto original:")
    print(repr(problematic_text))
    
    cleaned_text = TextCleaner.clean_for_excel(problematic_text)
    
    print("\nTexto limpio:")
    print(repr(cleaned_text))
    
    print("\nTexto limpio (legible):")
    print(cleaned_text)

if __name__ == "__main__":
    test_cleaner()