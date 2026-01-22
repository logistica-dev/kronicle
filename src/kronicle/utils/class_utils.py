# kronicle/utils/class_utils.py
class classproperty:
    def __init__(self, cls_prop):
        self._cls_prop = cls_prop

    def __get__(self, obj, cls):
        return self._cls_prop(cls)
