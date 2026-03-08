<?php

declare(strict_types=1);

namespace ScImportDumps\Tests\Unit\Service;

use ScImportDumps\Service\DiffService;

/**
 * Tests for DiffService
 */
class DiffServiceTest extends AbstractServiceTestCase
{
    private function makeService(): DiffService
    {
        return new DiffService($this->connection, $this->prefix);
    }

    public function testCompareTablesReturnsErrorWhenTargetTableMissing(): void
    {
        // SHOW COLUMNS for target returns empty (table does not exist)
        $this->connection
            ->method('fetchAllAssociative')
            ->willReturn([]);

        $service = $this->makeService();
        $result = $service->compareTables('nonexistent');

        $this->assertNotNull($result['error']);
        $this->assertStringContainsString('does not exist', $result['error']);
    }

    public function testCompareTablesReturnsErrorWhenSourceTempTableMissing(): void
    {
        // First call: SHOW COLUMNS for target (returns columns)
        // Second call: SHOW COLUMNS for source temp table (returns empty)
        $this->connection
            ->method('fetchAllAssociative')
            ->willReturnOnConsecutiveCalls(
                [['Field' => 'id_product'], ['Field' => 'name']], // target columns
                [], // source columns (empty - table missing)
            );

        $service = $this->makeService();
        $result = $service->compareTables('product');

        $this->assertNotNull($result['error']);
        $this->assertStringContainsString('does not exist', $result['error']);
    }

    public function testCompareTablesDetectsColumnsOnlyInSource(): void
    {
        $this->connection
            ->method('fetchAllAssociative')
            ->willReturnOnConsecutiveCalls(
                [['Field' => 'id_product'], ['Field' => 'name']], // target columns
                [['Field' => 'id_product'], ['Field' => 'name'], ['Field' => 'extra_col']], // source columns (has extra)
                [], // SHOW KEYS (no PK found easily, return empty)
            );

        $this->mockFetchOneSequence([0, 0]);

        $service = $this->makeService();
        $result = $service->compareTables('product');

        $this->assertNull($result['error']);
        $this->assertContains('extra_col', $result['columns_only_in_source']);
        $this->assertEmpty($result['columns_only_in_target']);
    }

    public function testCompareTablesDetectsColumnsOnlyInTarget(): void
    {
        $this->connection
            ->method('fetchAllAssociative')
            ->willReturnOnConsecutiveCalls(
                [['Field' => 'id_product'], ['Field' => 'name'], ['Field' => 'target_only']], // target columns
                [['Field' => 'id_product'], ['Field' => 'name']], // source columns
                [], // SHOW KEYS
            );

        $this->mockFetchOneSequence([0, 0]);

        $service = $this->makeService();
        $result = $service->compareTables('product');

        $this->assertNull($result['error']);
        $this->assertContains('target_only', $result['columns_only_in_target']);
        $this->assertEmpty($result['columns_only_in_source']);
    }

    public function testCompareTablesDetectsMissingRowsInTarget(): void
    {
        $targetColumns = [['Field' => 'id_product'], ['Field' => 'name']];
        $sourceColumns = [['Field' => 'id_product'], ['Field' => 'name']];
        $showKeys = [['Column_name' => 'id_product']];

        // fetchAllAssociative call sequence:
        // 1. SHOW COLUMNS target
        // 2. SHOW COLUMNS source (temp table)
        // 3. SHOW KEYS
        // 4. fetchFirstColumn for target PKs
        // 5. Fetch source rows
        $this->connection
            ->method('fetchAllAssociative')
            ->willReturnOnConsecutiveCalls(
                $targetColumns,
                $sourceColumns,
                $showKeys,
                // fetchAllAssociative for source rows
                [
                    ['id_product' => 1, 'name' => 'Existing'],
                    ['id_product' => 2, 'name' => 'Missing'],
                ],
            );

        // fetchFirstColumn returns target PKs (only id_product=1 exists in target)
        $this->connection
            ->method('fetchFirstColumn')
            ->willReturn([1]);

        // fetchOne for row counts
        $this->mockFetchOneSequence([2, 1]);

        $service = $this->makeService();
        $result = $service->compareTables('product');

        $this->assertNull($result['error']);
        $this->assertSame(2, $result['rows_count_source']);
        $this->assertSame(1, $result['rows_count_target']);
        $this->assertCount(1, $result['rows_missing_in_target']);
        $this->assertSame(2, $result['rows_missing_in_target'][0]['id_product']);
    }

    public function testCompareTablesReturnsNoMissingRowsWhenTargetIsComplete(): void
    {
        $columns = [['Field' => 'id_product'], ['Field' => 'name']];
        $showKeys = [['Column_name' => 'id_product']];

        $this->connection
            ->method('fetchAllAssociative')
            ->willReturnOnConsecutiveCalls(
                $columns,
                $columns,
                $showKeys,
                [['id_product' => 1, 'name' => 'Product A']],
            );

        // Both PKs exist in target
        $this->connection
            ->method('fetchFirstColumn')
            ->willReturn([1]);

        $this->mockFetchOneSequence([1, 1]);

        $service = $this->makeService();
        $result = $service->compareTables('product');

        $this->assertNull($result['error']);
        $this->assertEmpty($result['rows_missing_in_target']);
    }

    public function testCompareMultipleReturnsReportForAllTables(): void
    {
        // Two tables, both will fail (no columns returned) to keep mock simple
        $this->connection
            ->method('fetchAllAssociative')
            ->willReturn([]);

        $service = $this->makeService();
        $report = $service->compareMultiple(['product', 'category']);

        $this->assertArrayHasKey('product', $report);
        $this->assertArrayHasKey('category', $report);
        $this->assertNotNull($report['product']['error']);
        $this->assertNotNull($report['category']['error']);
    }

    public function testCompareMultipleReturnsEmptyReportForEmptyTableList(): void
    {
        $service = $this->makeService();
        $report = $service->compareMultiple([]);

        $this->assertSame([], $report);
    }
}
