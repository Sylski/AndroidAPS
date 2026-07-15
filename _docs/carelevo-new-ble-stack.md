# CareLevo — New BLE Stack: Architecture, Status & Migration Roadmap

Coroutine + `CompletableDeferred` BLE client built alongside the legacy Rx driver. Phase 1 (adopt the
shared `BleTransport`) is done; **BleClient still has zero production call sites**.

> **Refreshed 2026-07-14.** Since this plan was written the *whole* CareLevo driver was moved onto the AAPS
> CommandQueue lifecycle (connect→execute→disconnect owned by the QueueWorker) — but on the **legacy Rx
> BLE stack**, not this one. That work surfaced a worse form of the very race this client fixes: a lost
> correlation event on the single queue-worker thread **deadlocks the whole queue** (freezes all
> boluses/basals/status). It is currently **band-aided** (not fixed) by an `awaitOnIo` helper in the
> executor. This new stack is the structural fix. See **Status (2026-07-14)** and the reframed **Phase 2**
> below.

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
```

> **2026-07-14:** the dev smoke-test (`compose/smoketest/CarelevoBleSmokeTest.kt` +
> `CarelevoBleSmokeTestDialog` + the "[Dev] BLE smoke test" button on `CarelevoPatchFlowStep01Start`) was
> **deleted**. So the new stack no longer has *any* runtime entry point — it is exercised only by unit
> tests. Wiring it into production is exactly Phase 2 (below).

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

**`BleClient` still has zero production call sites** — `BleClientImpl` is compiled into the APK, wired to
`BleTransport` (Phase 1), and exercised only by unit tests. As of 2026-07-14 it has **no runtime entry
point at all** (the dev smoke-test that used to reach it was deleted). Two concrete commands exist
(`MacAddressCommand`, `ImmediateBolusCommand`); ~28 more remain to be written.

**What actually runs in production is the legacy Rx stack — now wrapped by the CommandQueue.** Every pump
op flows `CarelevoPumpPlugin` (Pump interface) → `CommandQueue` → `CarelevoActivationExecutor` /
`CarelevoConnectionCoordinator`, which drive the legacy `CarelevoBleMangerImpl`/`CarelevoBleControllerImpl`
(≈1003 + 253 LOC) + the `PublishSubject + blockingFirst` correlation in the domain use cases. So the queue
now *owns the lifecycle*, but the *BLE transport + correlation is still the legacy Rx path this document
plans to replace.*

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
`BleClient` now runs on the shared `BleTransport`. No production behaviour change — and, since the dev
smoke-test was removed, no runtime exerciser at all until Phase 2 wires it in. `CarelevoBleTransportImpl`
is a near-clone of the hardware-proven Equil impl, so its first real hardware validation will be the
first migrated (read-only) use case in Phase 2.

Adversarial review applied: the adapter now self-guards post-`close()` calls with a `closed` flag
(fail fast instead of hang, mirroring `AndroidGattConnection`'s `gatt ?: throw`), `require`s the
`uuid` args match its single write/notify chars (mismatch fails loudly), and the transport logs a
warning if its single listener slot is overwritten without a prior `close()` (Phase-2 reconnect guard).
Known deferred (matches `AndroidGattConnection`, not a regression): the `writeAck`/`discoveryAck`/
`descriptorAck` fields are keyed by "current op" not a per-op token — a stale late ack could in
theory complete a newer op's deferred. Shared follow-up for both classes someday.

### Status (2026-07-14): the CommandQueue conversion changed the Phase-2 landscape

Between Phase 1 and now, a separate, large effort (`_docs/CARELEVO_COMMANDQUEUE_CONVERSION.md`, P0–P8)
put the whole driver under the AAPS CommandQueue — **on the legacy Rx BLE stack**. Net effect on this
roadmap:

- **The Pump-interface boundary is now the queue, not a direct method.** Pump ops → `CommandQueue` →
  `CarelevoActivationExecutor.execute(customCommand)` (13 `runX` handlers) + `CarelevoConnectionCoordinator`.
- **The deadlock this document is about is now live and only band-aided.** Each `runX` handler calls
  `awaitOnIo(single, sec) = single.subscribeOn(io).timeout(sec).blockingGet()`. Reason: the domain use
  cases are `Single.fromCallable { … patchObserver.patchEvent.ofType(…).blockingFirst() … }` with **no
  `subscribeOn`**, so without `awaitOnIo` the `blockingFirst()` runs *inline on the single queue-worker
  thread* and `.timeout()` cannot interrupt it → a lost/late correlation event **hangs the whole queue
  forever** (all delivery frozen). `awaitOnIo` moves the block to IO so the timeout fires — it prevents the
  catastrophic hang but does **not** fix the lost-event race. `runSafetyCheck` similarly `blockingSubscribe`s
  a progress `Observable`.
- **`CarelevoConnectionCoordinator` is now queue-shaped but still Rx.** `disconnect()` really drops the
  link (`bleController.execute(Disconnect).timeout(3s).blockingGet()`); `isConnected()` is the strict
  fully-ready `btState.isConnected()` the QueueWorker gates on; `startReconnection()` is **still
  fire-and-forget nested-`subscribe` Rx** (single-flight `AtomicBoolean` + a 10s `btState` observer
  timeout as the only backstop). The QueueWorker just polls `connect()`/`isConnected()` until the async
  chain lands — a silent reconnect failure relies on that 10s timeout to avoid a stuck poll.
- **`CarelevoPatch` was thinned** (`observeSyncPatch` + `updateMaxBolusDose`/`updateLowInsulinNoticeAmount`
  deleted; those settings are now queue commands). Its state is **8 `BehaviorSubject<Optional<X>>`** — no
  `StateFlow`/`LiveData` inside it (those live in the ViewModels).

**Bottom line:** the queue conversion is *complementary* to this migration (its "connect-before-execute /
single blocking boundary" is exactly where a suspend `BleClient` belongs), but it also raised the stakes —
the `awaitOnIo` band-aid and the fire-and-forget `startReconnection` are Rx debt that Phase 2 is now the
right way to pay off.

### Phase 2 — legacy-Rx retirement (reframed for the queue)

Wire `BleClient` into production and delete the legacy stack (`CarelevoBleMangerImpl` **≈1003 LOC** +
`CarelevoBleControllerImpl` **≈253** + `CarelevoBleSource` + `CarelevoPatchObserver`'s `PublishSubject`s),
rewiring the executor + `CarelevoConnectionCoordinator`. Sub-steps (dependencies noted — this is **not** a
strict 1→7 order):

1. **Extend `BleClient` API** — multi-response (`0x33→0x93+0x94`) + streaming/`Flow<R>` (`0x12` SafetyCheck).
   ✅ **DONE 2026-07-14** (unit-tested, device regression pass, uncommitted on `carelevo`). Added
   `BleMultiCommand`/`BleStreamCommand` interfaces + `requestMultiple(cmd): R` (completes when the full
   opcode SET has arrived, order-independent, first-per-opcode wins) + `requestStream(cmd): Flow<R>`
   (emits each decoded notification, completes on the command's `isTerminal`). `BleClientImpl` refactored
   to a sealed `Waiter` (`Single`/`Multi`/`Stream`), still single-in-flight under `requestMutex`. A
   43-agent adversarial review (15 confirmed / 23 refuted) then drove a hardening pass — all fixed:
   guarded consumer `decode`/`isTerminal` + a collector-wide backstop so no callback throw can brick the
   sole event router; `unsolicitedEvents` → `DROP_OLDEST`+`tryEmit` (router can't be back-pressured);
   `waiter` → `AtomicReference` w/ `getAndSet` on abort (lock-free, no clobber-without-abort race);
   `offer` returns real consumption so late duplicates fall through to unsolicited. `BleClientContractTest`
   11/11 unchanged (single path intact) + `BleClientExtendedContractTest` 21 new (incl. isTerminal-throw /
   decode-throw / cancel-frees-mutex / write-failure / empty-set-guard). Still **zero production call
   sites** — the phone install only regression-proves the untouched legacy path.
2. **Bulk-write remaining commands AND convert the use cases off `Single`.** ~28 commands (only 2 exist),
   each unit-testable vs `FakeGattConnection`. Crucially this step turns the use cases from
   `Single.fromCallable{blockingFirst()}` into **`suspend fun`** (or `Flow` for SafetyCheck) — which is
   what lets `awaitOnIo` be deleted (see step 6). *Depends on 1.*
3. **Migrate first repository** behind a feature flag — read-only patch info first (low blast radius),
   legacy path kept for instant rollback. *Depends on 2.*
4. **End-to-end equivalence tests** (~6) at `CarelevoPumpPlugin` level over a faked `BleTransport`, run
   against both stacks to prove equal `PumpEnactResult`. *Alongside 3.*
5. **State consolidation** — migrate `CarelevoPatch`'s **`BehaviorSubject<Optional<X>>` → `StateFlow<X?>`**
   (the "+ StateFlow + LiveData triple" in the old plan was wrong — `CarelevoPatch` is BehaviorSubject-only;
   the `LiveData` lives in `CarelevoOverviewViewModel`). *Independent of 1–4; do any time.*
6. **Executor + coordinator de-Rx (the awaitOnIo/startReconnection payoff).** Once the use cases are
   `suspend` (step 2): (a) collapse the 13 `awaitOnIo` calls into **one `runBlocking` at
   `executeCustomCommand`**, each `runX` becoming a plain `suspend` `withTimeout { useCase.execute() }`;
   (b) convert `startReconnection` from fire-and-forget nested-`subscribe` to a **suspend** sequence
   (`bleClient` Connect → discover → enable-notifications) that `connect()` actually **awaits**, removing
   the poll-until-`isConnected` silent-failure window. *Depends on 2.*
7. **Drop RxJava.** Blocked until 2 + 6 **and** the `bleController.execute(Single)` seam is gone —
   `bleController` is a legacy Rx island; either wrap it `suspend` or (preferred) let `BleClient` sit
   directly on `BleTransport` so `bleController.execute` becomes obsolete. Only then remove the RxJava
   dependency from `:pump:carelevo`. *Final.*

Suggested order: **1 → 2 (+4 alongside) → 6 → 3 → 5 → 7** (5 can slot in anywhere).

### Correction (2026-07-14): "swap one read behind a flag" is impossible — migrate by connection SESSION

A 4-investigator research pass found the incremental-slice premise doesn't survive the code:
- **`0x33` patch-info is dead code in production.** The only `0x93`+`0x94` read is inside activation and is
  elicited by a **SET TIME `0x11`** write, not `0x33`. The standalone status read (`getPumpStatus`/
  `readStatus`) reads *infusion* info (`0x31→0x91`). So there is no isolated legacy patch-info read to migrate.
- **The two stacks are mutually-exclusive GATT owners.** `CarelevoBleTransportImpl` opens its OWN
  `BluetoothGatt` (own callback, own `connectGatt` at :260), independent of legacy `CarelevoBleMangerImpl`
  (:350). No shared handle; notifications flow only to the GATT client that subscribed. So **`BleClient`
  cannot read over the legacy-owned link** — the new transport must open+establish its OWN connection. Two
  GATTs to one patch = the status-133 collision the code already warns about.
- **Therefore the migration unit is a whole connection *session*, not one read.** A flag switches connection
  OWNERSHIP for a session, not a single exchange.

**Corrected strategy — staged full migration (not big-bang; a pump can't have its first hardware test be the
whole driver):**
- **2.A — Session backbone + hardware-validation gate.** New `CarelevoBleSession` builds a **fresh**
  `BleTransportGattConnection`+`BleClientImpl`+scope **per connection** (the adapter's `close()` is one-shot
  and releases the transport listener, so per-session-fresh avoids bricking a singleton), doing
  connect→discover→enable-notifications→one exchange→close. First real command `PatchInfoCommand :
  BleMultiCommand` (0x33→{0x93,0x94}). A flag-gated `CmdReadPatchInfoV2` customCommand drives it on the Pixel
  (legacy link dropped + reconnect suppressed first) — proving the new stack end-to-end AND empirically
  answering "does a reconnect to an activated patch need app-auth replay?" (legacy reconnect stops at
  EnableNotifications → likely no; verify on device). Flag: `CARELEVO_USE_NEW_BLE_STACK` (`engineeringModeOnly`).
  ✅ **DONE + DEVICE-VALIDATED 2026-07-14** (Pixel 9a): `newBle.readPatchInfo OK serial=EO12507099001 fw=T168
  model=6776514848` — full new stack (connect own-GATT → discover → enable-notifications → write 0x33 → receive
  0x93+0x94 → correlate → decode) in ~200 ms after dropping the legacy link. **Auth question ANSWERED: no
  app-auth replay is needed on reconnect to an activated patch** (connect→discover→enable-notifications sufficed).
  Two on-device fixes: MAC must be `.uppercase()`d for `getRemoteDevice`; real 0x94 frame is 16 bytes so RPT2
  decode length/model-range relaxed (model clamps to bytes 11..15). No reconnect-suppression latch needed (the
  QueueWorker is blocked inside the read, so it can't re-dial legacy concurrently).
- **2.B — Build out commands + convert use cases to `suspend`** (vs `FakeGattConnection`), still flag-gated.
  ▶ **STARTED + first command DEVICE-VALIDATED 2026-07-14:** `InfusionInfoCommand` (single-response `0x31→0x91`) —
  flag-on `getPumpStatus` now does the REAL status read over the new stack (`newBle.readInfusionInfo OK
  remains=293.45 basal=1.2 bolus=3.35 pumpState=0 mode=2`), persisting through the SAME seam as legacy via new
  `CarelevoPatch.applyInfusionInfoReport` (shared `dispatchInfusionInfo`; identical pumpState/mode enum-roundtrip) →
  like-for-like, non-degrading. Proves the single-response `request()` path on hardware. ~26 commands remain.
- **2.C — Rewire coordinator+executor to the session; delete `awaitOnIo`; flip default** (legacy behind flag-off).
  Executor `customCommand` ops all done (settings + activation: needle/safety/alarm×2/set-basal). The
  **delivery-path** portion (bolus/temp-basal/extended + cancels, through the coordinators) has its own detailed
  plan in [`CARELEVO_DELIVERY_MIGRATION.md`](CARELEVO_DELIVERY_MIGRATION.md) — per-op fresh sessions (no
  persistent-connection rewire needed), sequenced D1 temp-basal → D2 extended → D3 immediate-bolus; bolus cancel
  = Option A (per-op cancel session).
- **2.D — Delete legacy + flag + RxJava; re-enable the disabled patch-info test on `BleClient`.**
- **Emulator** (parallel) makes 2.B/2.C CI-testable without hardware.

Open questions from 2.A — RESOLVED on device: **auth-on-reconnect = none needed** (a fresh GATT read succeeded
with connect→discover→enable-notifications only); **reconnect-suppression = not needed** (the QueueWorker is
blocked inside the customCommand/status-read, so it can't concurrently re-dial legacy). The two legacy decode
quirks (`bootDateTime` fabricated from the phone clock; `modelName` decimal-not-ASCII with byte 10 skipped) are
PRESERVED for like-for-like output (e.g. real model `"CL300"` → legacy/new both emit `"6776514848"`).

**Emulator (deferred, near-free once on `BleTransport`).** A `:pump:carelevo-emulator` module mirroring
`equil-emulator`: a `CarelevoEmulatorBleTransport` swapped in via DI, driving `onCharacteristicChanged`
to simulate the pump. Because `BleClient` sits on `BleGatt`/`BleTransportListener`, the emulator plugs
in at the same seam and the correlation layer works unchanged — removing the hardware gate for CI. (Also
the only realistic way to CI-test the alarm/warning flows, which need pump-pushed events.)

Each phase ships a working driver; no long-lived broken intermediate state.

## Known non-issues (reviewed, deliberately deferred)

- `FakeGattConnection._writes` + `ArrayDeque`s not thread-safe — safe today because `BleClientImpl.requestMutex` serializes callers. Revisit if future tests bypass `BleClient`.
- `MacAddressResponse` hex format diverges from legacy `0xAA0xBB...` — intentional; no consumers wired yet; repository migration will add a formatter if needed.
- `AndroidGattConnection` doesn't include: reconnect/retry policy, MTU negotiation, bonding coordination, gatt refresh on abnormal disconnect, OEM quirk workarounds. Legacy `CarelevoBleMangerImpl` has all of these — `AndroidGattConnection` is a minimal skeleton for the request/response path only.

## Why this matters

The legacy `PublishSubject + blockingFirst` correlation has a lost-event race (response arrives before the
`blockingFirst` subscribe → event dropped → the request never completes). This is not theoretical:

- The still-`@Disabled` `CarelevoConnectNewPatchUseCaseTest.execute_retries_round_when_serial_is_empty` is
  exactly this pattern. Once repositories move to `BleClient` it can be rewritten (each round a fresh
  `CompletableDeferred`, no shared subject state) and re-enabled.
- Under the CommandQueue conversion the same race got **worse**: a lost event on the single queue-worker
  thread doesn't just fail one request — it **deadlocks the entire queue** (every subsequent bolus, basal,
  SMB and status read never runs). That is the `awaitOnIo` band-aid's reason for existing; it caps the
  damage with a timeout but the request still spuriously fails on a dropped event.

`BleClient` removes the race at the root by registering the `CompletableDeferred` waiter **before**
`writeCharacteristic` (proven by contract test *"response delivered synchronously during writeCharacteristic
is not lost"*). Completing Phase 2 therefore lets `awaitOnIo`, the per-op timeout guesswork, and the
fire-and-forget reconnect all be simplified or deleted — it is the structural fix the band-aids stand in for.
