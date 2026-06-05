import importlib

mod = importlib.import_module("scripts.migration.01_migrate")
run_migration = mod.run_migration
