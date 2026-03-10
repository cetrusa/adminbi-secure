class BimboRouter:
    """
    Database router para la app bimbo.

    - Modelos unmanaged (AgenciaBimbo) → conexión 'bimbo' (powerbi_bimbo)
    - Modelos managed (PermisoBimboAgente) → conexión 'default' (powerbi_adm)
    """

    BIMBO_APP = "bimbo"

    def db_for_read(self, model, **hints):
        if getattr(model._meta, "app_label", None) == self.BIMBO_APP:
            if not model._meta.managed:
                return "bimbo"
        return None

    def db_for_write(self, model, **hints):
        if getattr(model._meta, "app_label", None) == self.BIMBO_APP:
            if not model._meta.managed:
                return "bimbo"
        return None

    def allow_relation(self, obj1, obj2, **hints):
        app1 = getattr(obj1._meta, "app_label", None)
        app2 = getattr(obj2._meta, "app_label", None)
        if app1 == self.BIMBO_APP or app2 == self.BIMBO_APP:
            return True
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        if app_label == self.BIMBO_APP:
            return db == "default"
        return None
