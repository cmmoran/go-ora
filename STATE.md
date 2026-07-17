# STATE

## Open Items
- Design staged Oracle native JSON support around `json.RawMessage`/OSON interoperability before considering the broader experimental v3 type/coder architecture.
- Evaluate OCI IAM token authentication and direct AQ support against concrete consumer requirements; do not port the current upstream APIs without token-refresh, security, cancellation, and live Oracle coverage.
- Complete the compatibility migration away from direct assignment to the exported `advanced_nego.NTSAuth` variable, then remove the legacy process-global auth hooks in a future major release.
- Migrate callers from legacy `dbms.NewOutput(*sql.DB, ...)` to `NewOutputContext(*sql.Conn, ...)`; remove the affinity-unsafe constructor in a future major release.
- Replace the now-bounded redirect/refuse/resend recursion with iterative state machines and add fake-transport/fuzz coverage.
- Incrementally split statement execution, connection lifecycle, and packet transport responsibilities after characterization tests cover the wire protocol paths.
- Define non-panicking error contracts for the exported network integer-writing helpers without breaking existing callers.

## In Progress
- None.

## Finished Tasks
- Preserved output destinations for zero-row DML RETURNING statements and returned a successful zero-row result instead of a spurious pointer-type error.
- Corrected pointer-slice vector construction so encoded vectors no longer contain prefixed zero elements or doubled counts.
- Isolated negotiated string converters per connection while preserving explicitly configured converter overrides.
- Added bounded, connector-scoped in-memory wallet loading with malformed-input and nil-reader hardening.
- Validated the UUID/RAW(16) integration paths against a live Oracle Free 23.26.1 database, including a race-enabled run.
- Stabilized UUID/RAW(16) support with UUID-first string inference, strict canonical/compact parsing, raw-byte normalization for UUID-like valuers, and explicit `VarChar`/`NVarChar` overrides.
- Added `UUIDString` as a canonical string representation that binds and scans Oracle `RAW(16)`, plus live-validated Oracle integration coverage using Google UUID types.
- Synchronized the shared custom-type registry and connection bad-state flag, with race-detector coverage.
- Removed DSN parsing's process-global NTS side effect, added session-owned NTS configuration, and synchronized legacy auth setters.
- Added a pinned-session DBMS output API and made the broken legacy context helper fail explicitly.
- Bounded connect redirect/refuse and packet resend transitions with identifiable errors.
- Completed a repository-wide Principal Engineer review and implemented localized correctness, security, lifecycle, protocol-framing, observability, and API hardening.
- Added local validation for the Oracle TTC 65,535 bind/define-field limit.
- Corrected `BulkInsert` row/column handling and temporary-LOB cleanup chunk progression.
- Made connection opening transactional and TLS configuration connection-owned.
- Completed the initial uncommitted-delta review and resolved its custom-converter and quoted-comment blockers during the repository-wide pass.
- Established `STATE.md` tracking file with required sections and daily-log format.

## Daily Log
### 2026-07-17
- Corrected zero-row DML RETURNING handling by tracking whether Oracle returned each output value instead of clearing the caller's destination. Added unit coverage distinguishing no returned row from a returned SQL NULL and PL/SQL output, plus a live Oracle RAW(16)/Google UUID regression test.
- Validated the zero-row and existing nonzero UUID RETURNING paths normally and under the race detector against Oracle Free 23.26.1. Root, compatibility-module, integration-compilation, and non-Kerberos race suites passed; repository vet remains limited by the previously recorded unkeyed `database/sql` literals.
- Remediated the highest-value upstream parity findings in three focused commits: pointer-slice vector correctness, mixed-charset converter ownership, and connector-scoped wallet readers.
- Verified focused unit and race tests, root and compatibility-module suites, integration compilation, and the full non-Kerberos race suite. Re-ran the live UUID/RAW(16) integration test normally and under the race detector against Oracle Free 23.26.1; both sequential runs passed.
- Confirmed repository-wide vet remains limited to the pre-existing unkeyed `database/sql` literals and unreachable example code.
- Compared this descendant from its June 2025 common ancestor with `sijms/go-ora` master and the experimental `v3` module. Prioritized the vector correctness fix, mixed-charset connection isolation, in-memory wallets, and staged native JSON support; classified OCI IAM authentication and direct AQ as requirement-driven follow-up and fast login/the wholesale v3 codec rewrite as premature.
- Confirmed upstream v3 builds as a library but its current unit suite does not pass: root and converter tests no longer compile, and the `types` tests panic in reflection-based copy code. No v3 implementation was copied wholesale.
- Ran `TestUUIDRaw16RoundTrip` against `gvenzl/oracle-free:23.26.1-slim-faststart` on Docker's default context. Both the ordinary and race-enabled executions passed; the disposable container was removed afterward.
- Completed UUID/RAW(16) remediation in five focused commits: corrected UUID-like valuer and array encoding, consolidated strict string parsing, added the `VarChar` character-bind override, added `UUIDString`, and documented/tested Oracle integration behavior.
- Verified UUID unit tests and focused race tests. Compiled the integration module with the Google UUID round-trip test before completing live execution in a disposable Oracle Free container.
- Re-ran root and compatibility-module tests plus the root race suite after UUID remediation; all passed. Repository-wide vet remains limited to pre-existing unkeyed `database/sql` literals and unreachable example code after removing a UUID-test connection-copy warning.
- Completed the full-codebase architecture, correctness, concurrency, security, reliability, testing, maintainability, and performance review.
- Restored synchronized custom string-converter propagation and completed quote-aware SQL comment parsing.
- Corrected bulk-insert argument construction, zero-row behavior, shape validation, and temporary-LOB cleanup batching.
- Added all-or-nothing connection-open cleanup, TLS config cloning, wrapped-result-set validation, and tracer-only protocol diagnostics.
- Hardened TTC packet lengths and ACCEPT/REFUSE/REDIRECT/MARKER parsing against malformed network input.
- Added the 65,535 TTC field-count guard, explicit-auth precedence fix, and safe support for value-type `sql.Scanner` implementations.
- Verified `go test ./...`, `go test ./...` in `module_test`, and `CGO_ENABLED=1` race tests for all packages except the optional Kerberos example.
- Confirmed `go vet` still reports the deferred `dbms.EnableOutput` discarded-context defect; repository-wide vet also has older unkeyed-literal/example warnings.
- Live Oracle integration tests were not run because this environment does not have the required Oracle connection configuration.
- Remediated the remaining High findings: synchronized custom-type registration, session-owned NTS authentication, a pinned `*sql.Conn` DBMS output path, and bounded redirect/refuse/resend transitions.
- Remediated the Medium connection bad-state race with `atomic.Bool`.
- Verified focused unit and race tests, root `go test ./...`, `module_test/go test ./...`, and race tests for every package except the optional Kerberos example.
- Confirmed the optional Kerberos race build is blocked by missing system header `krb5.h`; repository-wide vet remains blocked by pre-existing unkeyed `database/sql` literals and unreachable example code.
- Reviewed UUID-like RAW(16) support end to end. Confirmed Google UUID scalar/output support is partly functional, identified broken 36-byte array/direct-driver encoding through `driver.Valuer`, unsafe global coercion of UUID-looking strings, permissive hyphen parsing, duplicated parsers, and missing live Oracle coverage.
- Organized the completed review/remediation work into local conventional commits: `e505773` value conversion/SQL parsing, `025422a` statement execution, `1e22880` network protocol handling, `4886af2` connection lifecycle/shared state, and `16be52f` DBMS output session affinity. No commits were pushed.

Key files changed/added/moved:
- changed: `command.go`, `command_test.go`, `parameter.go`, `STATE.md`
- added: `tests/dml_returning_test.go`
- changed: `configurations/wallet.go`, `connection.go`, `connection_test.go`, `driver.go`, `vector.go`, `STATE.md`
- added: `configurations/wallet_reader_test.go`, `vector_test.go`
- changed: `README.md`, `connection.go`, `converters/type_conversion.go`, `custom_types.go`, `parameter_encode.go`, `tests/go.mod`, `utils.go`, `value_getter.go`
- added: `converters/type_conversion_test.go`, `parameter_encode_test.go`, `tests/uuid_test.go`, `uuid.go`, `uuid_test.go`
- changed: `bulk_copy.go`, `command.go`, `command_test.go`, `configurations/connect_config.go`, `connection.go`, `driver.go`, `network/accept_packet.go`, `network/session.go`, `utils.go`, `utils_test.go`, `value_setter.go`, `STATE.md`
- changed: `advanced_nego/advanced_nego.go`, `advanced_nego/nts.go`, `dbms/output.go`, `parameter.go`, `parameter_encode.go`, `udt.go`
- added: `advanced_nego/auth_state_test.go`, `configurations/connect_config_test.go`, `connection_test.go`, `dbms/output_test.go`, `network/packet_validation_test.go`, `network/session_framing_test.go`, `network/session_tls_test.go`

### 2026-05-29
- Investigated production participant-service ORA-03120 failure and isolated it to a query with more than 65,535 bind parameters.
- Verified the Oracle TTC execute packet writes `len(stmt.Pars)` as a 2-byte value in `command.go`, matching the observed boundary: 65,535 UUID binds succeed and 65,536 fail with `ORA-03120`.

Key files changed/added/moved:
- changed: `STATE.md`

### 2026-02-16
- Performed a codebase health and risk assessment pass across core packages (`go_ora`, `network`, `configurations`, `advanced_nego`) with evidence collection for high-risk and high-cost maintenance hotspots.
- Cataloged structural concentration points, control-flow complexity, global state usage, observability gaps, and test-surface limitations.

Key files changed/added/moved:
- added: `STATE.md`
