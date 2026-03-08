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
 * Loads SQL dumps into temporary tables or creates PDO connections to external databases.
 */
class DumpLoaderService
{
    private const TEMP_TABLE_PREFIX = 'sc_dump_temp_';

    public function __construct(
        private readonly Connection $connection,
        private readonly string $dumpsDirectory,
        private readonly string $dbPrefix,
    ) {
    }

    /**
     * Load a SQL dump file into temporary tables named {prefix}sc_dump_temp_{tablename}.
     * Returns the list of table names loaded.
     *
     * @return array<string>
     *
     * @throws RuntimeException if the file cannot be read or parsed
     */
    public function loadFromFile(string $filePath): array
    {
        if (!file_exists($filePath) || !is_readable($filePath)) {
            throw new RuntimeException(sprintf('Dump file not found or not readable: %s', $filePath));
        }

        $fileSize = filesize($filePath);
        if ($fileSize === false || $fileSize > 256 * 1024 * 1024) {
            throw new RuntimeException(sprintf(
                'Dump file exceeds the 256 MB limit (actual: %s bytes): %s',
                $fileSize === false ? 'unknown' : number_format($fileSize),
                $filePath
            ));
        }

        $sql = file_get_contents($filePath);
        if ($sql === false) {
            throw new RuntimeException(sprintf('Cannot read dump file: %s', $filePath));
        }

        return $this->parseSqlAndCreateTempTables($sql);
    }

    /**
     * Create a PDO connection to an external database.
     * Credentials are used directly and never persisted.
     *
     * @param array{host: string, user: string, password: string, dbname: string, port?: int} $credentials
     *
     * @throws RuntimeException if connection fails
     */
    public function loadFromConnection(array $credentials): PDO
    {
        $host = $credentials['host'] ?? '';
        $user = $credentials['user'] ?? '';
        $password = $credentials['password'] ?? '';
        $dbname = $credentials['dbname'] ?? '';
        $port = (int) ($credentials['port'] ?? 3306);

        if (empty($host) || empty($user) || empty($dbname)) {
            throw new RuntimeException('Missing required connection credentials: host, user, dbname');
        }

        $dsn = sprintf('mysql:host=%s;port=%d;dbname=%s;charset=utf8mb4', $host, $port, $dbname);

        try {
            return new PDO($dsn, $user, $password, [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES utf8mb4',
            ]);
        } catch (\PDOException $e) {
            throw new RuntimeException(sprintf('Cannot connect to external database: %s', $e->getMessage()), 0, $e);
        }
    }

    /**
     * List available SQL dump files from the dumps directory.
     *
     * @return array<string> Filenames (not full paths)
     */
    public function getAvailableDumps(): array
    {
        if (!is_dir($this->dumpsDirectory)) {
            return [];
        }

        $files = glob($this->dumpsDirectory . '/*.sql');
        if ($files === false) {
            return [];
        }

        return array_map('basename', $files);
    }

    /**
     * Drop all temporary tables created by this service (sc_dump_temp_*).
     */
    public function cleanTempTables(): void
    {
        $tempPrefix = $this->dbPrefix . self::TEMP_TABLE_PREFIX;

        try {
            $tables = $this->connection->fetchAllAssociative(
                "SHOW TABLES LIKE ?",
                [$tempPrefix . '%']
            );

            foreach ($tables as $row) {
                $tableName = reset($row);
                if ($tableName !== false) {
                    $this->connection->executeStatement(
                        'DROP TABLE IF EXISTS `' . $tableName . '`'
                    );
                }
            }
        } catch (DbalException $e) {
            throw new RuntimeException(sprintf('Failed to clean temp tables: %s', $e->getMessage()), 0, $e);
        }
    }

    /**
     * Parse SQL content and create temporary tables from CREATE TABLE + INSERT statements.
     * Tables are created as {dbPrefix}sc_dump_temp_{originalName}.
     *
     * @return array<string> list of original table names loaded
     */
    private function parseSqlAndCreateTempTables(string $sql): array
    {
        $loadedTables = [];

        // Split into individual statements
        $statements = $this->splitSqlStatements($sql);

        foreach ($statements as $statement) {
            $statement = trim($statement);
            if (empty($statement)) {
                continue;
            }

            // Handle CREATE TABLE
            if (preg_match('/^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`"]?(\w+)[`"]?/i', $statement, $matches)) {
                $originalTable = $matches[1];
                $tempTable = $this->dbPrefix . self::TEMP_TABLE_PREFIX . $originalTable;

                // Replace table name in statement
                $modifiedStatement = $this->replaceTableName($statement, $originalTable, $tempTable);

                // Drop and recreate
                try {
                    $this->connection->executeStatement('DROP TABLE IF EXISTS `' . $tempTable . '`');
                    $this->connection->executeStatement($modifiedStatement);
                    $loadedTables[] = $originalTable;
                } catch (DbalException $e) {
                    throw new RuntimeException(
                        sprintf('Failed to create temp table for %s: %s', $originalTable, $e->getMessage()),
                        0,
                        $e
                    );
                }
            }

            // Handle INSERT INTO
            if (preg_match('/^\s*INSERT\s+(?:INTO\s+)?[`"]?(\w+)[`"]?/i', $statement, $matches)) {
                $originalTable = $matches[1];
                $tempTable = $this->dbPrefix . self::TEMP_TABLE_PREFIX . $originalTable;
                $modifiedStatement = $this->replaceTableName($statement, $originalTable, $tempTable);

                try {
                    $this->connection->executeStatement($modifiedStatement);
                } catch (DbalException $e) {
                    throw new RuntimeException(
                        sprintf('Failed to insert data into temp table for %s: %s', $originalTable, $e->getMessage()),
                        0,
                        $e
                    );
                }
            }
        }

        return array_unique($loadedTables);
    }

    /**
     * Split SQL content into individual statements by semicolons,
     * respecting string literals and comments.
     *
     * @return array<string>
     */
    private function splitSqlStatements(string $sql): array
    {
        // Remove -- comments and /* */ block comments for simplicity
        $sql = preg_replace('/--[^\n]*\n/', "\n", $sql) ?? $sql;
        $sql = preg_replace('/\/\*.*?\*\//s', '', $sql) ?? $sql;

        $statements = [];
        $current = '';
        $inString = false;
        $stringChar = '';
        $length = strlen($sql);

        for ($i = 0; $i < $length; ++$i) {
            $char = $sql[$i];

            if ($inString) {
                $current .= $char;
                if ($char === '\\') {
                    // Escape next character
                    if ($i + 1 < $length) {
                        $current .= $sql[++$i];
                    }
                } elseif ($char === $stringChar) {
                    $inString = false;
                }
            } else {
                if ($char === "'" || $char === '"' || $char === '`') {
                    $inString = true;
                    $stringChar = $char;
                    $current .= $char;
                } elseif ($char === ';') {
                    $statements[] = $current;
                    $current = '';
                } else {
                    $current .= $char;
                }
            }
        }

        if (trim($current) !== '') {
            $statements[] = $current;
        }

        return $statements;
    }

    /**
     * Replace a table name in a SQL statement with a new name.
     */
    private function replaceTableName(string $statement, string $oldName, string $newName): string
    {
        // Replace backtick-quoted name
        $statement = str_replace('`' . $oldName . '`', '`' . $newName . '`', $statement);

        // Replace unquoted name (word boundary)
        $statement = preg_replace(
            '/\b' . preg_quote($oldName, '/') . '\b/',
            '`' . $newName . '`',
            $statement
        ) ?? $statement;

        return $statement;
    }
}
