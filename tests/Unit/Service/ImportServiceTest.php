<?php

declare(strict_types=1);

namespace ScImportDumps\Tests\Unit\Service;

use Doctrine\DBAL\Exception as DbalException;
use ScImportDumps\Service\ImportService;

/**
 * Tests for ImportService
 */
class ImportServiceTest extends AbstractServiceTestCase
{
    private function makeService(): ImportService
    {
        return new ImportService($this->connection, $this->prefix);
    }

    // -------------------------------------------------------------------------
    // validateForeignKeys
    // -------------------------------------------------------------------------

    public function testValidateForeignKeysReturnsTrueWhenRowsEmpty(): void
    {
        $service = $this->makeService();
        $result = $service->validateForeignKeys('product', []);

        $this->assertTrue($result['valid']);
        $this->assertEmpty($result['errors']);
    }

    public function testValidateForeignKeysReturnsTrueWhenNoFkColumns(): void
    {
        // No id_lang / id_shop / id_currency columns in rows
        $rows = [
            ['id_product' => 1, 'name' => 'Foo'],
        ];

        // fetchFirstColumn should never be called when no FK values to check
        $this->connection->expects($this->never())->method('fetchFirstColumn');

        $service = $this->makeService();
        $result = $service->validateForeignKeys('product', $rows);

        $this->assertTrue($result['valid']);
        $this->assertEmpty($result['errors']);
    }

    public function testValidateForeignKeysReturnsTrueWhenAllFkValuesExist(): void
    {
        $rows = [
            ['id_product' => 1, 'id_lang' => 1, 'id_shop' => 1],
            ['id_product' => 2, 'id_lang' => 2, 'id_shop' => 1],
        ];

        // fetchFirstColumn is called 3 times (for lang, shop, currency)
        // lang => [1, 2], shop => [1], currency => (no id_currency in rows, skipped)
        $this->connection
            ->method('fetchFirstColumn')
            ->willReturnOnConsecutiveCalls(
                [1, 2],   // lang existing IDs
                [1, 2],   // shop existing IDs
            );

        $service = $this->makeService();
        $result = $service->validateForeignKeys('product_lang', $rows);

        $this->assertTrue($result['valid']);
        $this->assertEmpty($result['errors']);
    }

    public function testValidateForeignKeysReturnsFalseWhenLangIdMissing(): void
    {
        $rows = [
            ['id_product' => 1, 'id_lang' => 99, 'name' => 'Test'],
        ];

        // lang existing IDs does not include 99
        $this->connection
            ->method('fetchFirstColumn')
            ->willReturn([1, 2, 3]);

        $service = $this->makeService();
        $result = $service->validateForeignKeys('product_lang', $rows);

        $this->assertFalse($result['valid']);
        $this->assertNotEmpty($result['errors']);
        $this->assertStringContainsString('id_lang', $result['errors'][0]);
        $this->assertStringContainsString('99', $result['errors'][0]);
    }

    public function testValidateForeignKeysSkipsFkWhenReferenceTableMissing(): void
    {
        $rows = [
            ['id_product' => 1, 'id_currency' => 5],
        ];

        // fetchFirstColumn throws (reference table doesn't exist) => skip validation
        $this->connection
            ->method('fetchFirstColumn')
            ->willReturn([]);  // empty means table has no rows / doesn't exist

        $service = $this->makeService();
        // id_currency=5 but currency table has no rows: missing!
        $result = $service->validateForeignKeys('order', $rows);

        $this->assertFalse($result['valid']);
        $this->assertStringContainsString('id_currency', $result['errors'][0]);
    }

    // -------------------------------------------------------------------------
    // insertMissingRows
    // -------------------------------------------------------------------------

    public function testInsertMissingRowsReturnsZeroForEmptyRows(): void
    {
        $service = $this->makeService();
        $result = $service->insertMissingRows('product', []);

        $this->assertSame(0, $result['inserted']);
        $this->assertSame(0, $result['skipped']);
        $this->assertEmpty($result['errors']);
    }

    public function testInsertMissingRowsInsertsRowsAndReturnsCount(): void
    {
        $rows = [
            ['id_product' => 5, 'name' => 'New Product'],
        ];

        // No FK columns, no FK validation calls needed
        $this->connection->expects($this->never())->method('fetchFirstColumn');

        $this->connection
            ->expects($this->once())
            ->method('insert')
            ->willReturn(1);

        $service = $this->makeService();
        $result = $service->insertMissingRows('product', $rows, ['skip_fk_validation' => true]);

        $this->assertSame(1, $result['inserted']);
        $this->assertSame(0, $result['skipped']);
        $this->assertEmpty($result['errors']);
    }

    public function testInsertMissingRowsSkipsRowsWhenFkValidationFails(): void
    {
        $rows = [
            ['id_product' => 1, 'id_lang' => 999],
        ];

        // lang existing IDs does not include 999
        $this->connection
            ->method('fetchFirstColumn')
            ->willReturn([1, 2]);

        // insert must NOT be called
        $this->connection->expects($this->never())->method('insert');

        $service = $this->makeService();
        $result = $service->insertMissingRows('product_lang', $rows);

        $this->assertSame(0, $result['inserted']);
        $this->assertSame(1, $result['skipped']);
        $this->assertNotEmpty($result['errors']);
    }

    public function testInsertMissingRowsDryRunDoesNotInsert(): void
    {
        $rows = [
            ['id_product' => 1, 'name' => 'A'],
            ['id_product' => 2, 'name' => 'B'],
        ];

        $this->connection->expects($this->never())->method('insert');

        $service = $this->makeService();
        $result = $service->insertMissingRows('product', $rows, [
            'skip_fk_validation' => true,
            'dry_run' => true,
        ]);

        $this->assertSame(2, $result['inserted']);
        $this->assertSame(0, $result['skipped']);
    }

    public function testInsertMissingRowsRecordsErrorWhenInsertFails(): void
    {
        $rows = [
            ['id_product' => 1, 'name' => 'A'],
        ];

        // Use a generic DbalException to simulate any insert failure
        $dbalException = $this->createMock(\Doctrine\DBAL\Exception::class);

        $this->connection
            ->method('insert')
            ->willThrowException($dbalException);

        $service = $this->makeService();
        $result = $service->insertMissingRows('product', $rows, ['skip_fk_validation' => true]);

        $this->assertSame(0, $result['inserted']);
        $this->assertSame(1, $result['skipped']);
        $this->assertNotEmpty($result['errors']);
    }

    public function testInsertMissingRowsSkipFkValidationBypassesCheck(): void
    {
        $rows = [
            ['id_product' => 1, 'id_lang' => 999, 'name' => 'Test'],
        ];

        // fetchFirstColumn must NOT be called when skip_fk_validation is true
        $this->connection->expects($this->never())->method('fetchFirstColumn');
        $this->connection->expects($this->once())->method('insert')->willReturn(1);

        $service = $this->makeService();
        $result = $service->insertMissingRows('product', $rows, ['skip_fk_validation' => true]);

        $this->assertSame(1, $result['inserted']);
    }
}
