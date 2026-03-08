# ps8_import_dumps

PrestaShop 8 module — Compare SQL dumps or external databases with the current PrestaShop database and import missing data.

## Features

- Load SQL dump files from `modules/sc_import_dumps/dumps/` or upload via back office
- Connect to an external database (credentials session-only, never persisted)
- Multi-table diff report: column differences and missing rows (by primary key)
- Insert missing rows with pre-insert FK validation (id_lang, id_shop, id_currency)
- Temporary table cleanup after operations

## Use cases

- Post-migration data verification (compare source DB with migrated DB)
- Detect data loss between two PrestaShop instances
- Selective re-import of missing records

## Security

- External DB credentials used directly via PDO — never stored in ps_configuration or any file
- All SQL identifiers validated against `[a-zA-Z0-9_]` regex
- FK validation before any INSERT operation
- CSRF protection on write operations (import, clean)

## Requirements

- PrestaShop 8.x
- PHP 8.1+
- Doctrine DBAL (provided by PrestaShop)
- PDO MySQL extension

## Installation

Upload to `modules/sc_import_dumps/` and install from Back Office > Modules.

Registers under **Advanced Parameters > Scriptami** via the shared `AdminScriptami` parent tab.

## Architecture

```
src/
├── Controller/Admin/     # DumpsController (5 actions)
├── Service/              # DumpLoaderService, DiffService, ImportService
└── Traits/               # HaveScriptamiTab
```

## Tests

```bash
composer install
./vendor/bin/phpunit --testdox
```

29 tests, 70 assertions.

## Part of the Scriptami Suite

- [ps8_verify_multishop](https://github.com/RebelliousSmile/ps8_verify_multishop) — Multishop data integrity
- [ps8_replace_text](https://github.com/RebelliousSmile/ps8_replace_text) — Find & replace across the database
- [ps8_giftcard_repair](https://github.com/RebelliousSmile/ps8_giftcard_repair) — Gift card data repair
- [ps8_iqit_repair](https://github.com/RebelliousSmile/ps8_iqit_repair) — IQIT Warehouse theme module repair
- [ps8_import_dumps](https://github.com/RebelliousSmile/ps8_import_dumps) — SQL dump comparison and import
- [ps8_image_manager](https://github.com/RebelliousSmile/ps8_image_manager) — WebP conversion and image optimization
