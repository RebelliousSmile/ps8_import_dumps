<?php

declare(strict_types=1);

namespace ScImportDumps\Tests\Unit\Service;

use org\bovigo\vfs\vfsStream;
use RuntimeException;
use ScImportDumps\Service\DumpLoaderService;

/**
 * Tests for DumpLoaderService
 */
class DumpLoaderServiceTest extends AbstractServiceTestCase
{
    private string $dumpsDir;

    protected function setUp(): void
    {
        parent::setUp();
        $this->dumpsDir = sys_get_temp_dir() . '/sc_import_dumps_test_' . uniqid();
        mkdir($this->dumpsDir, 0777, true);
    }

    protected function tearDown(): void
    {
        // Cleanup temp dir
        $files = glob($this->dumpsDir . '/*') ?: [];
        foreach ($files as $file) {
            unlink($file);
        }
        if (is_dir($this->dumpsDir)) {
            rmdir($this->dumpsDir);
        }
    }

    private function makeService(): DumpLoaderService
    {
        return new DumpLoaderService(
            $this->connection,
            $this->dumpsDir,
            $this->prefix
        );
    }

    public function testGetAvailableDumpsReturnsEmptyWhenNoDumps(): void
    {
        $service = $this->makeService();
        $dumps = $service->getAvailableDumps();

        $this->assertSame([], $dumps);
    }

    public function testGetAvailableDumpsReturnsFileNames(): void
    {
        file_put_contents($this->dumpsDir . '/backup.sql', '-- sql');
        file_put_contents($this->dumpsDir . '/archive.sql', '-- sql');

        $service = $this->makeService();
        $dumps = $service->getAvailableDumps();

        $this->assertCount(2, $dumps);
        $this->assertContains('backup.sql', $dumps);
        $this->assertContains('archive.sql', $dumps);
    }

    public function testGetAvailableDumpsIgnoresNonSqlFiles(): void
    {
        file_put_contents($this->dumpsDir . '/backup.sql', '-- sql');
        file_put_contents($this->dumpsDir . '/readme.txt', 'text');

        $service = $this->makeService();
        $dumps = $service->getAvailableDumps();

        $this->assertCount(1, $dumps);
        $this->assertSame(['backup.sql'], $dumps);
    }

    public function testGetAvailableDumpsReturnsEmptyWhenDirectoryDoesNotExist(): void
    {
        $service = new DumpLoaderService($this->connection, '/nonexistent/path', $this->prefix);
        $dumps = $service->getAvailableDumps();

        $this->assertSame([], $dumps);
    }

    public function testLoadFromFileThrowsExceptionWhenFileNotFound(): void
    {
        $service = $this->makeService();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessageMatches('/not found or not readable/');

        $service->loadFromFile('/nonexistent/file.sql');
    }

    public function testLoadFromFileCreatesCreatesTempTable(): void
    {
        $sql = "CREATE TABLE `product` (`id_product` INT, `name` VARCHAR(255));";
        $filePath = $this->dumpsDir . '/test.sql';
        file_put_contents($filePath, $sql);

        // Expect: DROP TABLE IF EXISTS + CREATE TABLE
        $this->connection->expects($this->atLeast(2))
            ->method('executeStatement')
            ->willReturn(0);

        $service = $this->makeService();
        $result = $service->loadFromFile($filePath);

        $this->assertContains('product', $result);
    }

    public function testLoadFromFileReturnsUniqueTableNames(): void
    {
        $sql = implode("\n", [
            "CREATE TABLE `product` (`id_product` INT);",
            "INSERT INTO `product` VALUES (1);",
            "INSERT INTO `product` VALUES (2);",
        ]);
        $filePath = $this->dumpsDir . '/multi.sql';
        file_put_contents($filePath, $sql);

        $this->connection->method('executeStatement')->willReturn(0);

        $service = $this->makeService();
        $result = $service->loadFromFile($filePath);

        $this->assertSame(['product'], $result);
    }

    public function testLoadFromConnectionThrowsWhenMissingCredentials(): void
    {
        $service = $this->makeService();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessageMatches('/Missing required connection credentials/');

        $service->loadFromConnection(['host' => '', 'user' => '', 'dbname' => '', 'password' => '']);
    }

    public function testCleanTempTablesCallsDropForEachTempTable(): void
    {
        $tempTable = $this->prefix . 'sc_dump_temp_product';

        $this->connection
            ->expects($this->once())
            ->method('fetchAllAssociative')
            ->willReturn([['Tables_in_db' => $tempTable]]);

        $this->connection
            ->expects($this->once())
            ->method('executeStatement')
            ->with($this->stringContains('DROP TABLE IF EXISTS'));

        $service = $this->makeService();
        $service->cleanTempTables();
    }

    public function testCleanTempTablesDoesNothingWhenNoTempTables(): void
    {
        $this->connection
            ->expects($this->once())
            ->method('fetchAllAssociative')
            ->willReturn([]);

        $this->connection
            ->expects($this->never())
            ->method('executeStatement');

        $service = $this->makeService();
        $service->cleanTempTables();
    }
}
