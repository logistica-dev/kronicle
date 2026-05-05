class BootstrapReport:
    def __init__(self):
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.infos: list[str] = []

    def add_error(self, msg: str):
        self.errors.append(msg)

    def add_warning(self, msg: str):
        self.warnings.append(msg)

    def add_info(self, msg: str):
        self.infos.append(msg)

    @property
    def is_valid(self) -> bool:
        return not self.errors

    def raise_if_invalid(self):
        if self.errors:
            raise RuntimeError(self.format())

    def format(self) -> str:
        parts = []

        if self.errors:
            parts.append("Errors:\n" + "\n".join(self.errors))

        if self.warnings:
            parts.append("Warnings:\n" + "\n".join(self.warnings))

        return "\n\n".join(parts)
