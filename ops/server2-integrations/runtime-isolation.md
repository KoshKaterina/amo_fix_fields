# Runtime isolation of the primary integration containers

The compose override runs amo-fix-fields and woo-sklad as numeric UID/GID 65532, with a read-only root filesystem, all capabilities dropped, no-new-privileges and a 16 MiB /tmp. Docker JSON logs rotate at 10 MiB with five files.

Woo writes its optional file log under /tmp using WOO_LOG_FILE; application logs remain on stdout. The Woo source change is maintained in the woocommerce-sklad repository.

amo-fix-fields retains its existing /app/var bind mount. Preserve all live queues and ledgers. Before changing its UID, take consistent SQLite backups and a metadata backup with getfacl. Grant UID65532 read/write access to existing state, and default directory ACLs for files subsequently created by the still-root legacy Academy workers:

```sh
setfacl -R -m u:65532:rwX /opt/integrations/amo_fix_fields/var
find /opt/integrations/amo_fix_fields/var -type d -exec setfacl -m d:u:65532:rwx {} +
```

The acl OS package is required. Keep existing ownership and existing user/group permissions. Validate shared SQLite WAL and atomic JSON writes between root and UID65532 on an isolated copy before applying. A new producer that explicitly creates mode0600 files may mask a named ACL; review such code before deploying it.

Do not start another live business worker to test these settings. Offline import checks use --network none, a state copy and no lifespan. Verify Telegram getMe through the configured proxy immediately before the approved targeted restart, then health, the whole integration smoke and normal webhook delivery. Legacy Academy queues are separate; this configuration does not merge, delete, disable or migrate them.

Runtime rollback restores the saved compose override and recreates only amo-fix-fields. It uses the CURRENT state directory, never an older queue backup. Existing ACLs remain compatible with root; restore saved ACL metadata separately only after the nonroot service is no longer using it.

## Python dependency refresh, 2026-10-22

Security constraints pin FastAPI 0.159.1, Starlette 1.9.0, aiohttp 3.14.5, multidict 6.9.1, certifi 2026.10.19 and Pillow 13.0.0. Test each test file in a separate process with run_tests.py. Against the unchanged deployed application, baseline and candidate both produced 1636 passed, 13 failed and 13 errors across 69 files; the same five failing files concern missing operations sources and source-copy guard drift. This is a dependency regression comparison, not a fully green repository suite. Offline ASGI root health, image PDF generation and Telegram getMe through the configured proxy also passed.

The historical runtime image contained .env backup files. The build exclusions now cover .env* at every depth. The one-time production patch used the exact previous application bytes, removed these credential files in a builder and copied the sanitized filesystem into a scratch final stage; no historical secret-bearing layers are ancestors of the final image. All 338 Python source SHA values stayed equal; the final image has one filesystem layer, no .env files and no exact matches for the 17 known credential variable values under /app. This does not revoke previously exposed credentials or remove protected rollback images.

The protected deployment backup includes the prior image, config and fresh consistent SQLite snapshots. Runtime rollback retags the saved image and recreates only amo-fix-fields; it always keeps current queues. Never overwrite live state with the diagnostic snapshots.

For already hardened Academy containers, use harden_legacy_runtime.py with --dependency-refresh and an explicit --image after the image-specific checks. This uses a separate -deps-rollback name, retains the existing isolation and checks the invite queue for active leases before stopping the original. A live lease defers the operation. The dependency images reuse the tested sanitized Python runtime, preserve Sentry SDK 2.70.0 and copy each worker’s own exact /app tree. Baseline and candidate Academy tests: v11/v12/v13 each 74 passed; v14 94 passed, seven files per worker, no failures. All 137 application files per worker match byte-for-byte.
