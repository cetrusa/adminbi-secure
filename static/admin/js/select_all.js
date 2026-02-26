// Script para seleccionar/deseleccionar todas las empresas en el admin de Django
(function () {
    document.addEventListener('DOMContentLoaded', function () {
        var selectAll = document.getElementById('id_select_all_empresas');
        var empresas = document.querySelectorAll('input[name="conf_empresas"]');
        if (!selectAll || empresas.length === 0) return;

        // Cuando se marca/desmarca el checkbox de seleccionar todas
        selectAll.addEventListener('change', function () {
            empresas.forEach(function (checkbox) {
                checkbox.checked = selectAll.checked;
            });
        });

        // Si se desmarca alguna empresa manualmente, desmarcar el selectAll
        empresas.forEach(function (checkbox) {
            checkbox.addEventListener('change', function () {
                if (!checkbox.checked) {
                    selectAll.checked = false;
                } else {
                    // Si todas están marcadas, marcar selectAll
                    var allChecked = Array.from(empresas).every(function (cb) { return cb.checked; });
                    selectAll.checked = allChecked;
                }
            });
        });
    });
})();
