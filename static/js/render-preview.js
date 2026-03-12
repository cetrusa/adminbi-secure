/**
 * DataZenith Render Preview - Tabla de vista previa reutilizable
 *
 * Uso:
 *   renderPreview(headers, rows, options);
 *
 * Opciones:
 *   cardId:    ID del card contenedor (default: 'previewCard')
 *   headId:    ID del <tr> de headers (default: 'previewHead')
 *   bodyId:    ID del <tbody> (default: 'previewBody')
 *   infoId:    ID del <small> de info (default: 'preview-info')
 *   scroll:    Scroll al card al renderizar (default: true)
 */
(function (window) {
  'use strict';

  function renderPreview(headers, rows, options) {
    var opts = options || {};
    var cardId = opts.cardId || 'previewCard';
    var headId = opts.headId || 'previewHead';
    var bodyId = opts.bodyId || 'previewBody';
    var infoId = opts.infoId || 'preview-info';
    var scroll = opts.scroll !== undefined ? opts.scroll : true;

    var previewCard = document.getElementById(cardId);
    var headRow = document.getElementById(headId);
    var body = document.getElementById(bodyId);
    var info = document.getElementById(infoId);

    if (!previewCard || !headRow || !body) return;

    headRow.innerHTML = '';
    body.innerHTML = '';

    if (!headers || headers.length === 0 || !rows || rows.length === 0) {
      previewCard.style.display = 'none';
      return;
    }

    headers.forEach(function (h) {
      var th = document.createElement('th');
      th.textContent = h;
      headRow.appendChild(th);
    });

    rows.forEach(function (r) {
      var tr = document.createElement('tr');
      headers.forEach(function (h) {
        var td = document.createElement('td');
        td.textContent = (r[h] !== undefined && r[h] !== null) ? r[h] : '';
        tr.appendChild(td);
      });
      body.appendChild(tr);
    });

    if (info) {
      info.textContent = 'Mostrando ' + rows.length + ' de las primeras filas';
    }
    previewCard.style.display = 'block';

    if (scroll) {
      previewCard.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
  }

  window.renderPreview = renderPreview;

})(window);
