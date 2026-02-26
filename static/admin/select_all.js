document.addEventListener('DOMContentLoaded', function () {
    const selectAllCheckbox = document.querySelector('#id_select_all_empresas');
    const empresaCheckboxes = document.querySelectorAll('#id_conf_empresas input[type="checkbox"]');

    if (selectAllCheckbox) {
        selectAllCheckbox.addEventListener('change', function () {
            if (selectAllCheckbox.checked) {
                empresaCheckboxes.forEach(checkbox => checkbox.checked = true);
            } else {
                empresaCheckboxes.forEach(checkbox => checkbox.checked = false);
            }
        });
    }
});
