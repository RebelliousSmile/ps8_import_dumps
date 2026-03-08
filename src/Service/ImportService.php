<?php
/**
 * SC Import Dumps - PrestaShop 8 Module
 *
 * @author    Scriptami
 * @copyright Scriptami
 * @license   Academic Free License version 3.0
 */

declare(strict_types=1);

namespace ScImportDumps\Service;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception as DbalException;
use RuntimeException;

/**
 * Handles validation of foreign keys and insertion of missing rows into the target database.
 */
class ImportService
{
    /**
     * Foreign key columns and their corresponding reference tables (without prefix).
     *
     * @var array<string, string>
     */
    private const FK_MAP = [
        'id_lang' => 'lang',
        'id_shop' => 'shop',
        'id_currency' => 'currency',
    ];

    public function __construct(
        private readonly Connection $connection,
        private readonly string $dbPrefix,
    ) {
    }

    /**
     * Validate that FK values (id_lang, id_shop, id_currency) in $rows exist in target tables.
     *
     * @param array<array<string, mixed>> $rows
     *
     * @return array{valid: bool, errors: array<string>}
     */
    public function validateForeignKeys(string $table, array $rows): array
    {
        $errors = [];

        if (empty($rows)) {
            return ['valid' => true, 'errors' => []];
        }

        foreach (self::FK_MAP as $fkColumn => $refTable) {
            $fkValues = $this->extractFkValues($rows, $fkColumn);
            if (empty($fkValues)) {
                continue;
            }

            $existingIds = $this->getExistingIds($refTable, $fkColumn);
            $missing = array_diff($fkValues, $existingIds);

            if (!empty($missing)) {
                $errors[] = sprintf(
                    'Table `%s`: column `%s` references non-existing %s IDs: %s',
                    $table,
                    $fkColumn,
                    $refTable,
                    implode(', ', array_map('strval', $missing))
                );
            }
        }

        return [
            'valid' => empty($errors),
            'errors' => $errors,
        ];
    }

    /**
     * Insert missing rows into the target table after FK validation.
     *
     * @param array<array<string, mixed>> $rows
     * @param array{skip_fk_validation?: bool, dry_run?: bool} $options
     *
     * @return array{inserted: int, skipped: int, errors: array<string>}
     */
    public function insertMissingRows(string $table, array $rows, array $options = []): array
    {
        $result = [
            'inserted' => 0,
            'skipped' => 0,
            'errors' => [],
        ];

        if (empty($rows)) {
            return $result;
        }

        $targetTable = $this->dbPrefix . $table;
        $skipFkValidation = $options['skip_fk_validation'] ?? false;
        $dryRun = $options['dry_run'] ?? false;

        // Validate FK unless explicitly skipped
        if (!$skipFkValidation) {
            $fkResult = $this->validateForeignKeys($table, $rows);
            if (!$fkResult['valid']) {
                return [
                    'inserted' => 0,
                    'skipped' => count($rows),
                    'errors' => $fkResult['errors'],
                ];
            }
        }

        foreach ($rows as $row) {
            if ($dryRun) {
                ++$result['inserted'];
                continue;
            }

            try {
                $this->connection->insert('`' . $targetTable . '`', $row);
                ++$result['inserted'];
            } catch (DbalException $e) {
                ++$result['skipped'];
                $result['errors'][] = sprintf('Row insert failed: %s', $e->getMessage());
            }
        }

        return $result;
    }

    /**
     * Extract distinct non-null values for a given FK column from rows.
     *
     * @param array<array<string, mixed>> $rows
     *
     * @return array<int|string>
     */
    private function extractFkValues(array $rows, string $fkColumn): array
    {
        $values = [];
        foreach ($rows as $row) {
            if (isset($row[$fkColumn]) && $row[$fkColumn] !== null) {
                $values[] = $row[$fkColumn];
            }
        }

        return array_values(array_unique($values));
    }

    /**
     * Get existing IDs from a reference table for a given column.
     *
     * @return array<int|string>
     */
    private function getExistingIds(string $refTable, string $idColumn): array
    {
        $targetRefTable = $this->dbPrefix . $refTable;

        try {
            $ids = $this->connection->fetchFirstColumn(
                'SELECT `' . $idColumn . '` FROM `' . $targetRefTable . '`'
            );

            return $ids;
        } catch (DbalException) {
            // If reference table doesn't exist, skip validation for this FK
            return [];
        }
    }
}
