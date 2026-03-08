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
use PDO;
use RuntimeException;

/**
 * Compares tables between a source (temp tables or external PDO) and the target PrestaShop database.
 */
class DiffService
{
    private const TEMP_TABLE_PREFIX = 'sc_dump_temp_';

    public function __construct(
        private readonly Connection $connection,
        private readonly string $dbPrefix,
    ) {
    }

    /**
     * Compare a single table between source and target.
     *
     * When $sourceConn is null, source data is read from temporary tables loaded by DumpLoaderService.
     * When $sourceConn is provided, source data is read directly from the external database.
     *
     * @return array{
     *     columns_only_in_source: array<string>,
     *     columns_only_in_target: array<string>,
     *     rows_missing_in_target: array<array<string, mixed>>,
     *     rows_count_source: int,
     *     rows_count_target: int,
     *     primary_key: string|null,
     *     error: string|null
     * }
     */
    public function compareTables(string $table, ?PDO $sourceConn = null): array
    {
        $result = [
            'columns_only_in_source' => [],
            'columns_only_in_target' => [],
            'rows_missing_in_target' => [],
            'rows_count_source' => 0,
            'rows_count_target' => 0,
            'primary_key' => null,
            'error' => null,
        ];

        try {
            $targetTable = $this->dbPrefix . $table;

            // Get target columns
            $targetColumns = $this->getTargetColumns($targetTable);
            if (empty($targetColumns)) {
                $result['error'] = sprintf('Target table `%s` does not exist or has no columns', $targetTable);

                return $result;
            }

            // Get source columns
            $sourceColumns = $this->getSourceColumns($table, $sourceConn);
            if (empty($sourceColumns)) {
                $result['error'] = sprintf('Source table `%s` does not exist or has no columns', $table);

                return $result;
            }

            // Compare columns
            $result['columns_only_in_source'] = array_values(array_diff($sourceColumns, $targetColumns));
            $result['columns_only_in_target'] = array_values(array_diff($targetColumns, $sourceColumns));

            // Detect primary key from target
            $primaryKey = $this->getPrimaryKey($targetTable);
            $result['primary_key'] = $primaryKey;

            // Common columns for data comparison
            $commonColumns = array_intersect($sourceColumns, $targetColumns);

            // Count rows
            $result['rows_count_source'] = $this->countSourceRows($table, $sourceConn);
            $result['rows_count_target'] = $this->countTargetRows($targetTable);

            // Find rows missing in target (using PK)
            if ($primaryKey !== null && in_array($primaryKey, $commonColumns, true)) {
                $result['rows_missing_in_target'] = $this->findMissingRows(
                    $table,
                    $targetTable,
                    $primaryKey,
                    array_values($commonColumns),
                    $sourceConn
                );
            }
        } catch (\Throwable $e) {
            $result['error'] = $e->getMessage();
        }

        return $result;
    }

    /**
     * Compare multiple tables and return a report keyed by table name.
     *
     * @param array<string> $tables
     *
     * @return array<string, array{
     *     columns_only_in_source: array<string>,
     *     columns_only_in_target: array<string>,
     *     rows_missing_in_target: array<array<string, mixed>>,
     *     rows_count_source: int,
     *     rows_count_target: int,
     *     primary_key: string|null,
     *     error: string|null
     * }>
     */
    public function compareMultiple(array $tables, ?PDO $sourceConn = null): array
    {
        $report = [];
        foreach ($tables as $table) {
            $report[$table] = $this->compareTables($table, $sourceConn);
        }

        return $report;
    }

    /**
     * Get column names from the target (PrestaShop) database.
     *
     * @return array<string>
     */
    private function getTargetColumns(string $targetTable): array
    {
        try {
            $rows = $this->connection->fetchAllAssociative(
                'SHOW COLUMNS FROM `' . $targetTable . '`'
            );

            return array_column($rows, 'Field');
        } catch (DbalException) {
            return [];
        }
    }

    /**
     * Get column names from the source (temp table or external connection).
     *
     * @return array<string>
     */
    private function getSourceColumns(string $table, ?PDO $sourceConn): array
    {
        if ($sourceConn !== null) {
            try {
                $stmt = $sourceConn->query('SHOW COLUMNS FROM `' . $table . '`');
                if ($stmt === false) {
                    return [];
                }
                $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

                return array_column($rows, 'Field');
            } catch (\PDOException) {
                return [];
            }
        }

        // Read from temp table
        $tempTable = $this->dbPrefix . self::TEMP_TABLE_PREFIX . $table;
        try {
            $rows = $this->connection->fetchAllAssociative(
                'SHOW COLUMNS FROM `' . $tempTable . '`'
            );

            return array_column($rows, 'Field');
        } catch (DbalException) {
            return [];
        }
    }

    /**
     * Detect the primary key column of a target table.
     */
    private function getPrimaryKey(string $targetTable): ?string
    {
        try {
            $rows = $this->connection->fetchAllAssociative(
                'SHOW KEYS FROM `' . $targetTable . '` WHERE Key_name = "PRIMARY"'
            );

            return !empty($rows) ? (string) ($rows[0]['Column_name'] ?? '') ?: null : null;
        } catch (DbalException) {
            return null;
        }
    }

    /**
     * Count rows in the source table.
     */
    private function countSourceRows(string $table, ?PDO $sourceConn): int
    {
        if ($sourceConn !== null) {
            try {
                $stmt = $sourceConn->query('SELECT COUNT(*) FROM `' . $table . '`');
                if ($stmt === false) {
                    return 0;
                }

                return (int) $stmt->fetchColumn();
            } catch (\PDOException) {
                return 0;
            }
        }

        $tempTable = $this->dbPrefix . self::TEMP_TABLE_PREFIX . $table;
        try {
            $count = $this->connection->fetchOne('SELECT COUNT(*) FROM `' . $tempTable . '`');

            return (int) $count;
        } catch (DbalException) {
            return 0;
        }
    }

    /**
     * Count rows in the target table.
     */
    private function countTargetRows(string $targetTable): int
    {
        try {
            $count = $this->connection->fetchOne('SELECT COUNT(*) FROM `' . $targetTable . '`');

            return (int) $count;
        } catch (DbalException) {
            return 0;
        }
    }

    /**
     * Find rows that exist in source but not in target (based on primary key).
     *
     * @param array<string> $columns
     *
     * @return array<array<string, mixed>>
     */
    private function findMissingRows(
        string $sourceTable,
        string $targetTable,
        string $primaryKey,
        array $columns,
        ?PDO $sourceConn
    ): array {
        // Get all PKs from target
        try {
            $targetPks = $this->connection->fetchFirstColumn(
                'SELECT `' . $primaryKey . '` FROM `' . $targetTable . '`'
            );
        } catch (DbalException) {
            return [];
        }

        $columnList = implode(', ', array_map(fn ($c) => '`' . $c . '`', $columns));

        // Fetch all rows from source
        $sourceRows = $this->fetchAllSourceRows($sourceTable, $columnList, $sourceConn);

        // Filter rows whose PK is not in target
        return array_values(array_filter(
            $sourceRows,
            fn ($row) => !in_array($row[$primaryKey] ?? null, $targetPks, false)
        ));
    }

    /**
     * Fetch all rows from the source table.
     *
     * @return array<array<string, mixed>>
     */
    private function fetchAllSourceRows(string $table, string $columnList, ?PDO $sourceConn): array
    {
        if ($sourceConn !== null) {
            try {
                $stmt = $sourceConn->query('SELECT ' . $columnList . ' FROM `' . $table . '`');
                if ($stmt === false) {
                    return [];
                }

                return $stmt->fetchAll(PDO::FETCH_ASSOC);
            } catch (\PDOException) {
                return [];
            }
        }

        $tempTable = $this->dbPrefix . self::TEMP_TABLE_PREFIX . $table;
        try {
            return $this->connection->fetchAllAssociative(
                'SELECT ' . $columnList . ' FROM `' . $tempTable . '`'
            );
        } catch (DbalException) {
            return [];
        }
    }
}
