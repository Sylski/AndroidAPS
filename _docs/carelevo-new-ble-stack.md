# CareLevo — New BLE Stack: Architecture, Status & Migration Roadmap

Coroutine + `CompletableDeferred` BLE client built alongside the legacy Rx driver; smoke-test reachable via a dev button, no production integration yet.

## What exists

A new coroutine-first BLE stack built alongside the legacy `PublishSubject + blockingFirst` driver. Lives in `pump/carelevo/src/main/kotlin/app/aaps/pump/carelevo/ble/`:

```
ble/
├── BleClient.kt              — interface + BleCommand + BleResponse + UnsolicitedMessage + BleDisconnectedException
├── BleClientImpl.kt          — Mutex-serialized; registers @Volatile waiter BEFORE writing; opcode + optional correlationByte match
├── CarelevoBleTransport.kt   — [Phase 1] pump-local BleTransport (adds scanAddress, onGattError133)
├── CarelevoBleTransportImpl.kt — [Phase 1] production impl over Android GATT; near-clone of EquilBleTransportImpl
├── commands/
│   ├── MacAddressCommand.kt  — 0x3B→0x9B, read-only, uses clean uppercase hex (not legacy 0xAA0xBB format)
│   └── ImmediateBolusCommand.kt — 0x24→0x84, actionId correlation, BigDecimal HALF_UP rounding
└── gatt/
    ├── GattConnection.kt     — abstraction: SharedFlow<GattEvent> + write/discover/enableNotifications/close
    ├── AndroidGattConnection.kt — bespoke Android BluetoothGatt wrapper; smoke-test only, superseded by the adapter below
    └── BleTransportGattConnection.kt — [Phase 1] adapter: GattConnection over the shared BleTransport (keeps BleClient unchanged)

compose/smoketest/
└── CarelevoBleSmokeTest.kt   — runMacAddressSmokeTest() + CarelevoBleSmokeTestDialog Composable
```

Tests in `pump/carelevo/src/test/kotlin/app/aaps/pump/carelevo/ble/`:
- `BleClientContractTest.kt` — 11 tests (spec-as-tests)
- `commands/MacAddressCommandTest.kt` — 5 tests
- `commands/ImmediateBolusCommandTest.kt` — 15 tests
- `gatt/FakeGattConnection.kt` — scriptable test fixture

**Why:** the legacy driver's `PublishSubject + blockingFirst` correlation races (response arrives before subscribe → event lost → forever hang). New stack eliminates this by registering the `CompletableDeferred` waiter *before* calling `gatt.writeCharacteristic`. Proven by contract test `10 response delivered synchronously during writeCharacteristic is not lost`.

## Correlation rules

- Primary: opcode match (`BleCommand.expectedResponseOpcode` vs `payload[0]`)
- Secondary (bolus-family only): `correlationByte` == `payload[1]` (actionId echo check — rejects stale/mis-routed responses)
- Unsolicited: any notification that doesn't match the active waiter → `unsolicitedEvents: SharedFlow<UnsolicitedMessage>`
- Disconnect: `ConnectionStateChanged(DISCONNECTED)` aborts waiter with `BleDisconnectedException` and clears `waiter` reference immediately so late notifications route to unsolicited instead of being dropped

## Integration status

**Zero production call sites.** `BleClientImpl` and `AndroidGattConnection` are compiled into the APK but never invoked by any normal user flow. The only entry point is the dev smoke-test dialog.

Smoke test hook: `CarelevoPatchFlowStep01Start.kt` has a "[Dev] BLE smoke test" TextButton near the bottom. Tap opens `CarelevoBleSmokeTestDialog`, user enters MAC, tap Run, dialog opens its own `AndroidGattConnection`, calls `MacAddressCommand`, displays result. Dialog is reachable only on the pairing start screen (when no patch is paired).

## Patterns NOT yet supported by BleClient

Two legitimate protocol patterns not expressible with current `request(cmd): R` API:

1. **Multi-response per request.** `PatchInformationInquiry` (0x33) → pump sends RPT1 (0x93) + RPT2 (0x94). Current `BleCommand` has singular `expectedResponseOpcode`. Needs `requestMultiple` or similar.
2. **Streaming / progress events.** `SafetyCheck` (0x12) emits progress (REP_REQUEST) → eventually SUCCESS. Needs `Flow<R>` return or progress callback.

Design extension required before porting these commands. Simple single-response commands (SetTime, TempBasal, BolusCancel, etc.) work with current API unchanged.

## Transport abstraction: adopt the shared `BleTransport` (decided 2026-07-09)

The fleet already has a shared, coroutine-native BLE transport abstraction:
`app.aaps.core.interfaces.pump.ble.BleTransport` (`BleAdapter` + `BleScanner` + `BleGatt` +
`BleTransportListener` + `PairingState`). Dana (`BleTransportImpl` + `danars-emulator`), Equil
(`EquilBleTransportImpl` + `equil-emulator`) and Medtrum (`MedtrumBleTransportImpl`) all run on it,
with hardware-free emulator implementations for integration testing.

carelevo's bespoke `GattConnection`/`AndroidGattConnection` is a parallel reinvention of `BleGatt`.
**Decision: carelevo adopts `BleTransport` as its transport, keeping the (cleaner-than-fleet)
`BleClient` correlation layer on top.** Equil is the closest template — a patch-style pump with a
single service / single write char / single notify char, the exact shape carelevo needs.

Note: the Rx→coroutine correlation fix lives entirely in `BleClient` and is transport-agnostic.
`BleTransport` adoption is about **code unification** (and a later, near-free emulator), not the race fix.

### How it layers — Option A (chosen): `BleClient` untouched, one adapter

`BleClientImpl` depends only on the `GattConnection` interface, so we keep that seam and add one
adapter instead of rewriting the client:

- `CarelevoBleTransport : BleTransport` — pump-local interface (adds `scanAddress`, `onGattError133`), mirrors `EquilBleTransport`.
- `CarelevoBleTransportImpl` — near-clone of `EquilBleTransportImpl`; resolves tx(notify `e1b40003`)+rx(write `e1b40002`) from service `e1b40001` in `onServicesDiscovered`; fans the single `BluetoothGattCallback` out to one `BleTransportListener`.
- `BleTransportGattConnection : GattConnection, BleTransportListener` — the bridge: registers as the transport's single listener, turns each callback into the `GattEvent`/`CompletableDeferred` that `GattConnection` promises. `AndroidGattConnection` is its internal template.
- DI: `CarelevoBleModule` provides `CarelevoBleTransport`.

`BleClientImpl` / all 11 contract tests: **zero changes.**

Not chosen — Option B (make `BleClientImpl` implement `BleTransportListener` directly, delete
`GattConnection`): rewrites the client + its tests and downgrades carelevo's clean
transport/correlation/codec split to Equil's monolithic consumer shape. No functional gain.

### Accepted caveat

`BleGatt.writeCharacteristic(data)` is fire-and-forget `Unit`; `BleTransportListener.onCharacteristicWritten()`
carries no status. So a write the BLE stack silently drops is no longer fast-failed with
`GattWriteException` — it degrades to the caller's `withTimeout(...)`, which every `BleClient.request`
already applies. Equil ships with this exact behaviour. The *not-connected* case is still fast-failed
(the transport calls `onConnectionStateChanged(false)` synchronously from the write).

## Migration roadmap (revised 2026-07-09)

**Phase 1 — transport unification (DONE 2026-07-09, ~450 LOC, purely additive, build + 172 tests green).**
Added `CarelevoBleTransport` + `CarelevoBleTransportImpl` + `BleTransportGattConnection` + DI +
`BleTransportGattConnectionTest` (14 tests incl. end-to-end `BleClient`-over-adapter correlation).
`BleClient` now runs on the shared `BleTransport`. No production behaviour change yet — still driven
only by the dev smoke test. Hardware validation: point the smoke test at the transport path (a few
lines) when a pump is on hand; `CarelevoBleTransportImpl` is a near-clone of the hardware-proven Equil impl.

Adversarial review applied: the adapter now self-guards post-`close()` calls with a `closed` flag
(fail fast instead of hang, mirroring `AndroidGattConnection`'s `gatt ?: throw`), `require`s the
`uuid` args match its single write/notify chars (mismatch fails loudly), and the transport logs a
warning if its single listener slot is overwritten without a prior `close()` (Phase-2 reconnect guard).
Known deferred (matches `AndroidGattConnection`, not a regression): the `writeAck`/`discoveryAck`/
`descriptorAck` fields are keyed by "current op" not a per-op token — a stale late ack could in
theory complete a newer op's deferred. Shared follow-up for both classes someday.

**Phase 2 — legacy-Rx retirement (larger, higher-risk, follow-up).** Wire `BleClient` into production
command paths and delete the legacy stack (`CarelevoBleMangerImpl` ~1026 LOC + `CarelevoBleControllerImpl`
~258 + `CarelevoBleSource`), rewiring `CarelevoConnectionCoordinator`'s connect/reconnect/timeout.
Sub-steps:

1. **Extend BleClient API** for multi-response (`0x33→0x93+0x94`) + streaming (`0x12`) before bulk-writing commands.
2. **Bulk-write remaining commands** — ~30 total, each ~70 lines, unit-testable against `FakeGattConnection`.
3. **Migrate first repository** behind a feature flag. Start read-only (patch info) for low blast radius; keep the legacy path for instant rollback.
4. **End-to-end equivalence tests** — small set (~6) at `CarelevoPumpPlugin` level with a faked `GattConnection`/`BleTransport` below, run against both stacks to prove equivalent `PumpEnactResult`.
5. **State consolidation** — collapse `BehaviorSubject<Optional<X>>` + `StateFlow<X?>` + LiveData triple in `CarelevoPatch` to StateFlow-only.
6. **Coordinator simplification** — thin suspend orchestrators; single `runBlocking(IO)` at the Pump-interface boundary.
7. **Drop RxJava dependency** — final cleanup once all consumers migrated.

**Emulator (deferred, near-free once on `BleTransport`).** A `:pump:carelevo-emulator` module mirroring
`equil-emulator`: a `CarelevoEmulatorBleTransport` swapped in via DI, driving `onCharacteristicChanged`
to simulate the pump. Because `BleClient` sits on `BleGatt`/`BleTransportListener`, the emulator plugs
in at the same seam and the correlation layer works unchanged — removing the hardware gate for CI.

Each phase ships a working driver; no long-lived broken intermediate state.

## Known non-issues (reviewed, deliberately deferred)

- `FakeGattConnection._writes` + `ArrayDeque`s not thread-safe — safe today because `BleClientImpl.requestMutex` serializes callers. Revisit if future tests bypass `BleClient`.
- `MacAddressResponse` hex format diverges from legacy `0xAA0xBB...` — intentional; no consumers wired yet; repository migration will add a formatter if needed.
- `AndroidGattConnection` doesn't include: reconnect/retry policy, MTU negotiation, bonding coordination, gatt refresh on abnormal disconnect, OEM quirk workarounds. Legacy `CarelevoBleMangerImpl` has all of these — `AndroidGattConnection` is a minimal skeleton for the request/response path only.

## Why this matters

The disabled test `CarelevoConnectNewPatchUseCaseTest.execute_retries_round_when_serial_is_empty` — the one preserved as `@Disabled` — is exactly the pattern this new stack fixes. Once repositories migrate to `BleClient`, the test can be rewritten (each round uses a fresh `CompletableDeferred`, no shared subject state) and re-enabled.
