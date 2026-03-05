# scripts/utils/logger.py


def log_d(here: str, *args):
    print(f"D [{here}]", *args)


def log_w(here: str, *args):
    print(f"W [{here}]", *args)


def log_e(here: str, *args):
    print(f"E [{here}]", *args)
