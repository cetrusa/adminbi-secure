/* =========================================================================
   DataZenith Theme Toggle - Dark / Light mode switch
   ========================================================================= */
(function () {
    var STORAGE_KEY = 'dz-theme';

    function getPreferredTheme() {
        var stored = localStorage.getItem(STORAGE_KEY);
        if (stored) return stored;
        // Default: dark (mantiene comportamiento original)
        return 'dark';
    }

    function applyTheme(theme) {
        document.documentElement.setAttribute('data-bs-theme', theme);
        localStorage.setItem(STORAGE_KEY, theme);

        // Actualizar icono del botón (sol en dark → indica "cambiar a light", luna en light)
        var icon = document.getElementById('themeToggleIcon');
        if (icon) {
            icon.className = theme === 'dark' ? 'fas fa-sun' : 'fas fa-moon';
        }

        // Actualizar estilo del botón toggle
        var btn = document.getElementById('themeToggleBtn');
        if (btn) {
            btn.style.backgroundColor = theme === 'dark'
                ? 'rgba(255,255,255,0.08)'
                : 'rgba(0,0,0,0.06)';
        }

        // Swap de logo según tema
        var logo = document.getElementById('navbarLogo');
        if (logo) {
            logo.src = theme === 'dark'
                ? logo.getAttribute('data-logo-dark')
                : logo.getAttribute('data-logo-light');
        }
    }

    // Aplicar tema al cargar
    applyTheme(getPreferredTheme());

    // Exponer función toggle global
    window.toggleTheme = function () {
        var current = document.documentElement.getAttribute('data-bs-theme');
        applyTheme(current === 'dark' ? 'light' : 'dark');
    };

    // Escuchar cambios de preferencia del sistema (solo si no hay preferencia guardada)
    if (window.matchMedia) {
        window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', function (e) {
            if (!localStorage.getItem(STORAGE_KEY)) {
                applyTheme(e.matches ? 'dark' : 'light');
            }
        });
    }
})();
