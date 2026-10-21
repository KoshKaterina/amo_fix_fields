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
