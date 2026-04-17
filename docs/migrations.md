# Schema Migrations

Schema migrations are versioned in `src/migrations/feishu/registry.py`.

Default command:

```bash
python scripts/migrate_schema.py
```

This prints the pending migration plan and does not mutate Feishu.

Other schema checks:

```bash
python scripts/migrate_schema.py check-live
python scripts/migrate_schema.py expectations
```

To mark migrations as applied in local state after the Feishu tables/fields have been created:

```bash
python scripts/migrate_schema.py --apply
```

State is stored in `.data/schema_migrations.json`.

Current policy: migrations are check/documentation-first. Actual Feishu field creation remains manual until write-safe migration operations are implemented.
