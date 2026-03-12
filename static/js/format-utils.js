/**
 * format-utils.js
 * Utilidades compartidas de formateo numerico para DataZenith.
 * Locale: es-CO (Colombia) - separador de miles: punto, decimal: coma.
 *
 * Uso:
 *   <script src="{% static 'js/format-utils.js' %}"></script>
 *   formatCurrency(1234567)   => "$1.234.567"
 *   formatNumber(1234567)     => "1.234.567"
 *   formatPercent(85.5)       => "85,5%"
 */
(function (window) {
  'use strict';

  var LOCALE = 'es-CO';

  var _currencyFmt = new Intl.NumberFormat(LOCALE, {
    style: 'currency',
    currency: 'COP',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  });

  var _numberFmt = new Intl.NumberFormat(LOCALE, {
    maximumFractionDigits: 0,
  });

  var _percentFmt = new Intl.NumberFormat(LOCALE, {
    minimumFractionDigits: 1,
    maximumFractionDigits: 1,
  });

  /**
   * Formatea un valor como moneda colombiana (COP).
   * formatCurrency(1234567) => "$1.234.567"
   */
  function formatCurrency(value) {
    return _currencyFmt.format(Number(value) || 0);
  }

  /**
   * Formatea un numero con separadores de miles.
   * formatNumber(1234567) => "1.234.567"
   */
  function formatNumber(value) {
    return _numberFmt.format(Number(value) || 0);
  }

  /**
   * Formatea un porcentaje.
   * formatPercent(85.5) => "85,5%"
   */
  function formatPercent(value) {
    return _percentFmt.format(Number(value) || 0) + '%';
  }

  // Exponer globalmente
  window.formatCurrency = formatCurrency;
  window.formatNumber = formatNumber;
  window.formatPercent = formatPercent;

})(window);
