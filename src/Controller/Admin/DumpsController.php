<?php
/**
 * SC Import Dumps - PrestaShop 8 Module
 *
 * @author    Scriptami
 * @copyright Scriptami
 * @license   Academic Free License version 3.0
 */

declare(strict_types=1);

namespace ScImportDumps\Controller\Admin;

use PrestaShopBundle\Controller\Admin\FrameworkBundleAdminController;
use PrestaShopBundle\Security\Annotation\AdminSecurity;
use ScImportDumps\Service\DiffService;
use ScImportDumps\Service\DumpLoaderService;
use ScImportDumps\Service\ImportService;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;

/**
 * Admin controller for SC Import Dumps module.
 */
class DumpsController extends FrameworkBundleAdminController
{
    public function __construct(
        private readonly DumpLoaderService $dumpLoaderService,
        private readonly DiffService $diffService,
        private readonly ImportService $importService,
    ) {
    }

    /**
     * Dashboard: list available dumps + form for external DB connection.
     *
     * @AdminSecurity("is_granted('read', request.get('_legacy_controller'))")
     */
    public function indexAction(): Response
    {
        $availableDumps = $this->dumpLoaderService->getAvailableDumps();

        return $this->render('@Modules/sc_import_dumps/views/templates/admin/index.html.twig', [
            'available_dumps' => $availableDumps,
            'has_dumps' => !empty($availableDumps),
        ]);
    }

    /**
     * Handle SQL file upload (POST).
     *
     * @AdminSecurity("is_granted('update', request.get('_legacy_controller'))")
     */
    public function uploadAction(Request $request): Response
    {
        if (!$this->isCsrfTokenValid('sc_import_dumps_upload', $request->request->get('_token'))) {
            $this->addFlash('error', 'Invalid CSRF token.');

            return $this->redirectToRoute('sc_import_dumps_index');
        }

        $file = $request->files->get('dump_file');

        if ($file === null) {
            $this->addFlash('error', 'No file was uploaded.');

            return $this->redirectToRoute('sc_import_dumps_index');
        }

        $extension = strtolower($file->getClientOriginalExtension());
        if ($extension !== 'sql') {
            $this->addFlash('error', 'Only .sql files are accepted.');

            return $this->redirectToRoute('sc_import_dumps_index');
        }

        $dumpsDir = $this->getDumpsDirectory();
        $originalName = pathinfo($file->getClientOriginalName(), PATHINFO_FILENAME);
        $safeName = preg_replace('/[^a-zA-Z0-9_\-]/', '_', $originalName) . '.sql';

        try {
            $file->move($dumpsDir, $safeName);
            $this->addFlash('success', sprintf('File "%s" uploaded successfully.', $safeName));
        } catch (\Exception $e) {
            $this->addFlash('error', sprintf('Upload failed: %s', $e->getMessage()));
        }

        return $this->redirectToRoute('sc_import_dumps_index');
    }

    /**
     * Run the comparison (POST: tables + source selection).
     *
     * @AdminSecurity("is_granted('read', request.get('_legacy_controller'))")
     */
    public function compareAction(Request $request): Response
    {
        if (!$this->isCsrfTokenValid('sc_import_dumps_compare', $request->request->get('_token'))) {
            $this->addFlash('error', 'Invalid CSRF token.');

            return $this->redirectToRoute('sc_import_dumps_index');
        }

        $rawTables = $request->request->all('tables');
        $tables = array_values(array_filter(
            (array) $rawTables,
            static fn ($t) => is_string($t) && preg_match('/^[a-zA-Z0-9_]+$/', $t)
        ));
        $sourceType = $request->request->get('source_type', 'dump');
        $sourceFile = $request->request->get('source_file', '');

        $sourceConn = null;
        $report = [];
        $error = null;

        try {
            if ($sourceType === 'external') {
                $credentials = [
                    'host' => $request->request->get('ext_host', ''),
                    'user' => $request->request->get('ext_user', ''),
                    'password' => $request->request->get('ext_password', ''),
                    'dbname' => $request->request->get('ext_dbname', ''),
                    'port' => (int) $request->request->get('ext_port', '3306'),
                ];
                $sourceConn = $this->dumpLoaderService->loadFromConnection($credentials);
            } else {
                // Load from dump file
                $dumpsDir = $this->getDumpsDirectory();
                $filePath = $dumpsDir . '/' . basename($sourceFile);
                $this->dumpLoaderService->loadFromFile($filePath);
            }

            if (!empty($tables)) {
                $report = $this->diffService->compareMultiple((array) $tables, $sourceConn);
            }
        } catch (\Throwable $e) {
            $error = $e->getMessage();
        }

        return $this->render('@Modules/sc_import_dumps/views/templates/admin/compare.html.twig', [
            'report' => $report,
            'tables' => $tables,
            'source_type' => $sourceType,
            'source_file' => $sourceFile,
            'error' => $error,
        ]);
    }

    /**
     * Import missing rows (POST + CSRF).
     *
     * @AdminSecurity("is_granted('update', request.get('_legacy_controller'))")
     */
    public function importAction(Request $request): Response
    {
        if (!$this->isCsrfTokenValid('sc_import_dumps_import', $request->request->get('_token'))) {
            $this->addFlash('error', 'Invalid CSRF token.');

            return $this->redirectToRoute('sc_import_dumps_index');
        }

        $table = $request->request->get('table', '');
        if (!preg_match('/^[a-zA-Z0-9_]+$/', $table)) {
            $this->addFlash('error', 'Invalid table name.');

            return $this->redirectToRoute('sc_import_dumps_index');
        }

        $rowsJson = $request->request->get('rows_json', '[]');
        $skipFkValidation = (bool) $request->request->get('skip_fk_validation', false);

        $rows = json_decode($rowsJson, true) ?? [];
        $result = ['inserted' => 0, 'skipped' => 0, 'errors' => []];
        $error = null;

        try {
            $result = $this->importService->insertMissingRows($table, $rows, [
                'skip_fk_validation' => $skipFkValidation,
            ]);
        } catch (\Throwable $e) {
            $error = $e->getMessage();
        }

        return $this->render('@Modules/sc_import_dumps/views/templates/admin/import_result.html.twig', [
            'table' => $table,
            'result' => $result,
            'error' => $error,
        ]);
    }

    /**
     * Drop all temporary tables (POST + CSRF).
     *
     * @AdminSecurity("is_granted('delete', request.get('_legacy_controller'))")
     */
    public function cleanAction(Request $request): Response
    {
        if (!$this->isCsrfTokenValid('sc_import_dumps_clean', $request->request->get('_token'))) {
            $this->addFlash('error', 'Invalid CSRF token.');

            return $this->redirectToRoute('sc_import_dumps_index');
        }

        try {
            $this->dumpLoaderService->cleanTempTables();
            $this->addFlash('success', 'Temporary tables have been cleaned.');
        } catch (\Throwable $e) {
            $this->addFlash('error', sprintf('Failed to clean temp tables: %s', $e->getMessage()));
        }

        return $this->redirectToRoute('sc_import_dumps_index');
    }

    /**
     * Get the absolute path to the dumps directory.
     */
    private function getDumpsDirectory(): string
    {
        return dirname(__DIR__, 4) . '/dumps';
    }
}
