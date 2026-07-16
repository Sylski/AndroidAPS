package app.aaps.pump.carelevo.presentation.viewmodel

import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.pump.PumpEnactResult
import app.aaps.core.interfaces.queue.CommandQueue
import app.aaps.core.interfaces.rx.AapsSchedulers
import app.aaps.pump.carelevo.command.CarelevoActivationExecutor
import app.aaps.pump.carelevo.command.CmdAdditionalPriming
import app.aaps.pump.carelevo.command.CmdDiscard
import app.aaps.pump.carelevo.command.CmdSafetyCheck
import app.aaps.pump.carelevo.common.CarelevoPatch
import app.aaps.pump.carelevo.common.model.Event
import app.aaps.pump.carelevo.common.model.PatchState
import app.aaps.pump.carelevo.common.model.UiState
import app.aaps.pump.carelevo.domain.model.ResponseResult
import app.aaps.pump.carelevo.domain.model.patch.CarelevoPatchInfoDomainModel
import app.aaps.pump.carelevo.domain.model.result.ResultSuccess
import app.aaps.pump.carelevo.domain.type.SafetyProgress
import app.aaps.pump.carelevo.domain.usecase.CarelevoUseCaseResponse
import app.aaps.pump.carelevo.domain.usecase.patch.CarelevoPatchForceDiscardUseCase
import app.aaps.pump.carelevo.presentation.model.CarelevoConnectSafetyCheckEvent
import app.aaps.pump.carelevo.presentation.model.CarelevoOverviewEvent
import com.google.common.truth.Truth.assertThat
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.plugins.RxJavaPlugins
import io.reactivex.rxjava3.schedulers.Schedulers
import io.reactivex.rxjava3.subjects.BehaviorSubject
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import kotlinx.coroutines.test.resetMain
import kotlinx.coroutines.test.setMain
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.junit.jupiter.MockitoSettings
import org.mockito.kotlin.any
import org.mockito.kotlin.doSuspendableAnswer
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.mockito.quality.Strictness
import java.util.Optional

/**
 * JVM (non-Robolectric) unit tests for [CarelevoPatchSafetyCheckViewModel].
 *
 * The VM needs no Android framework surface — every collaborator is an interface/class that Mockito
 * can satisfy, and the only Android-ish dependency is `viewModelScope`, which resolves through
 * `Dispatchers.Main.immediate`. Installing [Dispatchers.Unconfined] as Main therefore makes the whole
 * VM testable on a plain JVM, and makes every `viewModelScope.launch` (the event emits, the
 * `setUiState` hops, the safety-check/discard/priming bodies) run EAGERLY and synchronously inside
 * the call that triggered them. That is why the tests can assert StateFlow values straight after
 * calling a VM method, with no `runTest`/`advanceUntilIdle`.
 *
 * Unconfined — NOT `UnconfinedTestDispatcher` — is required: the test dispatcher enters a coroutine
 * eagerly but queues every RESUMPTION on its TestCoroutineScheduler, so the progress collector would
 * subscribe, park, and never be resumed by an emit before `progressJob.cancel()` runs. Unconfined
 * resumes inline on the emitting thread, which is what makes the progress-frame tests deterministic.
 *
 * `commandQueue.customCommand` is a suspend fun; it is stubbed via [stubCustomCommand] (a
 * `runBlocking` wrapper around the normal `whenever`, matching `CarelevoPumpPluginStatusTest`) and
 * verified with `verifyBlocking`.
 *
 * Rx schedulers are stubbed to [Schedulers.trampoline] so the force-discard `Single` runs
 * synchronously on the test thread. Note the ticker started by the safety-check progress branch uses
 * `Observable.intervalRange`, which is hard-wired to the computation scheduler — so its ticks are
 * genuinely asynchronous. Assertions are therefore only made on values that are stable once the
 * (synchronous) terminal block has run: it disposes the ticker BEFORE writing the final
 * progress/remainSec, so no late tick can race the assertion.
 */
@OptIn(ExperimentalCoroutinesApi::class)
@ExtendWith(MockitoExtension::class)
@MockitoSettings(strictness = Strictness.LENIENT)
internal class CarelevoPatchSafetyCheckViewModelTest {

    @Mock lateinit var aapsSchedulers: AapsSchedulers
    @Mock lateinit var aapsLogger: AAPSLogger
    @Mock lateinit var carelevoPatch: CarelevoPatch
    @Mock lateinit var commandQueue: CommandQueue
    @Mock lateinit var activationExecutor: CarelevoActivationExecutor
    @Mock lateinit var patchForceDiscardUseCase: CarelevoPatchForceDiscardUseCase

    private lateinit var sut: CarelevoPatchSafetyCheckViewModel
    private lateinit var collectorScope: CoroutineScope

    /**
     * The one flow the SUT collects — stubbed once, before the VM is built, so tests never re-stub
     * `safetyProgress`. Tests drive the Progress branch by emitting into this instance.
     */
    private val progressFlow = MutableSharedFlow<SafetyProgress>(extraBufferCapacity = 16)

    private val address = "aa:bb:cc:dd:ee:ff"

    @BeforeEach
    fun setUp() {
        Dispatchers.setMain(Dispatchers.Unconfined)
        collectorScope = CoroutineScope(Dispatchers.Unconfined)
        // The force-discard Single ends in a 1-arg subscribe(onSuccess) with no onError, so a failing
        // Single routes an OnErrorNotImplementedException to the global Rx handler. Swallow it: the
        // doOnError side effects are what the test asserts.
        RxJavaPlugins.setErrorHandler { }

        whenever(aapsSchedulers.main).thenReturn(Schedulers.trampoline())
        whenever(aapsSchedulers.io).thenReturn(Schedulers.trampoline())
        // Default: a progress stream that never emits, so startSafetyCheck's collector parks and no
        // ticker is started — keeps the non-ticker tests fully deterministic. Overridden where the
        // Progress branch is under test.
        whenever(activationExecutor.safetyProgress).thenReturn(progressFlow)

        sut = CarelevoPatchSafetyCheckViewModel(
            aapsSchedulers = aapsSchedulers,
            aapsLogger = aapsLogger,
            carelevoPatch = carelevoPatch,
            commandQueue = commandQueue,
            activationExecutor = activationExecutor,
            patchForceDiscardUseCase = patchForceDiscardUseCase
        )
    }

    @AfterEach
    fun tearDown() {
        collectorScope.cancel()
        RxJavaPlugins.reset()
        Dispatchers.resetMain()
    }

    // ---- helpers ------------------------------------------------------------------------------

    /** Build the result mock BEFORE it is handed to a `thenReturn` (avoids UnfinishedStubbingException). */
    private fun enactResult(success: Boolean): PumpEnactResult = mock<PumpEnactResult>().also {
        whenever(it.success).thenReturn(success)
    }

    /** `customCommand` is suspend — record the stub from inside a coroutine. */
    private fun stubCustomCommand(result: PumpEnactResult) = runBlocking {
        whenever(commandQueue.customCommand(any())).thenReturn(result)
    }

    /**
     * Stub `customCommand` so it emits one Progress frame while "running", then returns [result].
     *
     * Both [yield]s are load-bearing, because `startSafetyCheck` launches its progress collector and
     * only THEN awaits `customCommand` — a stub that returns instantly is cancelled before the
     * collector ever runs. The first yield lets the collector get scheduled and subscribe (without it
     * subscriptionCount stays 0 and the replay-0 SharedFlow drops the emit); the second lets the
     * collector's dispatched resumption actually process the frame before the answer returns and the
     * terminal block cancels it. Together they reproduce the real queue round-trip's suspension.
     */
    private fun stubCustomCommandEmittingProgress(
        result: PumpEnactResult,
        onSeeded: () -> Unit = {}
    ) = runBlocking {
        whenever(commandQueue.customCommand(any())).doSuspendableAnswer {
            yield()
            progressFlow.emit(SafetyProgress.Progress(60L))
            yield()
            onSeeded()
            result
        }
    }

    /** Collect one-shot events from the moment of the call, so BOTH emits of a two-event flow land. */
    private fun collectEvents(): MutableList<Event> {
        val events = mutableListOf<Event>()
        collectorScope.launch { sut.event.collect { events += it } }
        return events
    }

    private fun collectProgress(): MutableList<Int?> {
        val values = mutableListOf<Int?>()
        collectorScope.launch { sut.progress.collect { values += it } }
        return values
    }

    private fun collectRemainSec(): MutableList<Long?> {
        val values = mutableListOf<Long?>()
        collectorScope.launch { sut.remainSec.collect { values += it } }
        return values
    }

    private fun givenPatchState(state: PatchState?) {
        val optional: Optional<PatchState> = if (state == null) Optional.empty() else Optional.of(state)
        whenever(carelevoPatch.patchState).thenReturn(BehaviorSubject.createDefault(optional))
    }

    private fun givenPatchInfo(info: CarelevoPatchInfoDomainModel?) {
        val optional: Optional<CarelevoPatchInfoDomainModel> = if (info == null) Optional.empty() else Optional.of(info)
        whenever(carelevoPatch.patchInfo).thenReturn(BehaviorSubject.createDefault(optional))
    }

    private fun patchInfo(checkSafety: Boolean?) =
        CarelevoPatchInfoDomainModel(address = address, checkSafety = checkSafety)

    // ---- defaults / isCreated -----------------------------------------------------------------

    @Test
    fun `initial state is idle with no progress`() {
        assertThat(sut.uiState.value).isEqualTo(UiState.Idle)
        assertThat(sut.progress.value).isNull()
        assertThat(sut.remainSec.value).isNull()
        assertThat(sut.isCreated).isFalse()
    }

    @Test
    fun `setIsCreated toggles the created latch`() {
        sut.setIsCreated(true)
        assertThat(sut.isCreated).isTrue()

        sut.setIsCreated(false)
        assertThat(sut.isCreated).isFalse()
    }

    // ---- triggerEvent / generateEventType -----------------------------------------------------

    @Test
    fun `triggerEvent forwards a known safety-check event unchanged`() {
        val events = collectEvents()

        sut.triggerEvent(CarelevoConnectSafetyCheckEvent.ShowMessageCarelevoIsNotConnected)

        assertThat(events).contains(CarelevoConnectSafetyCheckEvent.ShowMessageCarelevoIsNotConnected)
    }

    @Test
    fun `triggerEvent maps an unmapped safety-check event to NoAction`() {
        // NoAction is the only CarelevoConnectSafetyCheckEvent subtype generateEventType does not
        // enumerate, so it falls into the else branch (which itself returns NoAction).
        val events = collectEvents()

        sut.triggerEvent(CarelevoConnectSafetyCheckEvent.NoAction)

        assertThat(events).contains(CarelevoConnectSafetyCheckEvent.NoAction)
    }

    @Test
    fun `triggerEvent ignores events from another hierarchy`() {
        val events = collectEvents()

        sut.triggerEvent(CarelevoOverviewEvent.NoAction)

        assertThat(events).isEmpty()
    }

    // ---- startSafetyCheck ---------------------------------------------------------------------

    @Test
    fun `startSafetyCheck is refused when bluetooth is off`() {
        whenever(carelevoPatch.isBluetoothEnabled()).thenReturn(false)
        val events = collectEvents()

        sut.startSafetyCheck()

        assertThat(events).contains(CarelevoConnectSafetyCheckEvent.ShowMessageBluetoothNotEnabled)
        assertThat(sut.progress.value).isNull()
        verifyBlocking(commandQueue, never()) { customCommand(any()) }
    }

    @Test
    fun `startSafetyCheck completes with a full progress bar when the queue reports success`() {
        whenever(carelevoPatch.isBluetoothEnabled()).thenReturn(true)
        stubCustomCommand(enactResult(true))
        val events = collectEvents()

        sut.startSafetyCheck()

        assertThat(sut.progress.value).isEqualTo(100)
        assertThat(sut.remainSec.value).isEqualTo(0L)
        assertThat(events).contains(CarelevoConnectSafetyCheckEvent.SafetyCheckProgress)
        assertThat(events).contains(CarelevoConnectSafetyCheckEvent.SafetyCheckComplete)
    }

    @Test
    fun `startSafetyCheck routes a CmdSafetyCheck through the command queue`() {
        whenever(carelevoPatch.isBluetoothEnabled()).thenReturn(true)
        stubCustomCommand(enactResult(true))

        sut.startSafetyCheck()

        verifyBlocking(commandQueue) { customCommand(any<CmdSafetyCheck>()) }
    }

    @Test
    fun `startSafetyCheck fails without completing the progress bar when the queue reports failure`() {
        whenever(carelevoPatch.isBluetoothEnabled()).thenReturn(true)
        stubCustomCommand(enactResult(false))
        val events = collectEvents()

        sut.startSafetyCheck()

        assertThat(events).contains(CarelevoConnectSafetyCheckEvent.SafetyCheckFailed)
        assertThat(events).doesNotContain(CarelevoConnectSafetyCheckEvent.SafetyCheckComplete)
        // The failure branch must not fake a finished bar.
        assertThat(sut.progress.value).isNull()
        assertThat(sut.remainSec.value).isNull()
    }

    @Test
    fun `a safety-check progress frame seeds the countdown before the terminal success`() {
        whenever(carelevoPatch.isBluetoothEnabled()).thenReturn(true)
        val ok = enactResult(true)
        // progress/remainSec are CONFLATED StateFlows, so the seeded 0/60 is legitimately collapsed by
        // the terminal 100/0 that follows it — a collector can't reliably observe an intermediate that
        // is immediately overwritten. Sample the seed IN FLIGHT instead (inside the command answer,
        // after the frame has been processed but before the terminal block runs). That asserts the
        // ordering the UI depends on: the bar is seeded while the check runs, then completed.
        var seededProgress: Int? = null
        var seededRemain: Long? = null
        stubCustomCommandEmittingProgress(ok) {
            seededProgress = sut.progress.value
            seededRemain = sut.remainSec.value
        }
        val events = collectEvents()

        sut.startSafetyCheck()

        // Progress frame → bar reset to 0 and the countdown seeded with the frame's timeout...
        assertThat(seededProgress).isEqualTo(0)
        assertThat(seededRemain).isEqualTo(60L)
        // ...then the terminal success snaps it to a finished bar (the ticker is disposed first, so
        // no late tick can move these back).
        assertThat(sut.progress.value).isEqualTo(100)
        assertThat(sut.remainSec.value).isEqualTo(0L)
        assertThat(events).contains(CarelevoConnectSafetyCheckEvent.SafetyCheckComplete)
    }

    @Test
    fun `a safety-check progress frame is followed by a failure event when the queue rejects`() {
        whenever(carelevoPatch.isBluetoothEnabled()).thenReturn(true)
        val failed = enactResult(false)
        stubCustomCommandEmittingProgress(failed)
        val progressValues = collectProgress()
        val events = collectEvents()

        sut.startSafetyCheck()

        // The frame really did seed the bar (0), and the rejection must not then complete it.
        assertThat(progressValues).contains(0)
        assertThat(events).contains(CarelevoConnectSafetyCheckEvent.SafetyCheckFailed)
        assertThat(sut.progress.value).isNotEqualTo(100)
    }

    @Test
    fun `onSafetyCheckComplete finishes the bar and signals completion`() {
        val events = collectEvents()

        sut.onSafetyCheckComplete()

        assertThat(sut.progress.value).isEqualTo(100)
        assertThat(sut.remainSec.value).isEqualTo(0L)
        assertThat(events).contains(CarelevoConnectSafetyCheckEvent.SafetyCheckComplete)
    }

    // ---- startDiscardProcess: gating ----------------------------------------------------------

    @Test
    fun `startDiscardProcess short-circuits when the patch was never booted`() {
        givenPatchState(PatchState.NotConnectedNotBooting)
        val events = collectEvents()

        sut.startDiscardProcess()

        assertThat(events).contains(CarelevoConnectSafetyCheckEvent.DiscardComplete)
        assertThat(sut.uiState.value).isEqualTo(UiState.Idle)
        verifyBlocking(commandQueue, never()) { customCommand(any()) }
    }

    @Test
    fun `startDiscardProcess short-circuits when there is no patch state at all`() {
        givenPatchState(null)
        val events = collectEvents()

        sut.startDiscardProcess()

        assertThat(events).contains(CarelevoConnectSafetyCheckEvent.DiscardComplete)
        verifyBlocking(commandQueue, never()) { customCommand(any()) }
    }

    @Test
    fun `startDiscardProcess completes through the queue when the patch is reachable`() {
        givenPatchState(PatchState.ConnectedBooted)
        stubCustomCommand(enactResult(true))
        val events = collectEvents()

        sut.startDiscardProcess()

        assertThat(events).contains(CarelevoConnectSafetyCheckEvent.DiscardComplete)
        assertThat(sut.uiState.value).isEqualTo(UiState.Idle)
        verifyBlocking(commandQueue) { customCommand(any<CmdDiscard>()) }
        // The BLE teardown lives inside CmdDiscard on the queue thread, not in the VM.
        verify(carelevoPatch, never()).discardTeardown()
    }

    @Test
    fun `startDiscardProcess runs on the connected-not-booted branch too`() {
        givenPatchState(PatchState.ConnectedNoBooted)
        stubCustomCommand(enactResult(true))
        val events = collectEvents()

        sut.startDiscardProcess()

        assertThat(events).contains(CarelevoConnectSafetyCheckEvent.DiscardComplete)
        verifyBlocking(commandQueue) { customCommand(any<CmdDiscard>()) }
    }

    // ---- startDiscardProcess → force-discard fallback ------------------------------------------

    @Test
    fun `a rejected queue discard falls back to force-discard and tears the patch down`() {
        givenPatchState(PatchState.NotConnectedBooted)
        stubCustomCommand(enactResult(false))
        val success: ResponseResult<CarelevoUseCaseResponse> = ResponseResult.Success(ResultSuccess)
        whenever(patchForceDiscardUseCase.execute()).thenReturn(Single.just(success))
        val events = collectEvents()

        sut.startDiscardProcess()

        verify(carelevoPatch).discardTeardown()
        assertThat(events).contains(CarelevoConnectSafetyCheckEvent.DiscardComplete)
        assertThat(sut.uiState.value).isEqualTo(UiState.Idle)
    }

    @Test
    fun `a force-discard that errors leaves the patch intact and reports failure`() {
        givenPatchState(PatchState.ConnectedBooted)
        stubCustomCommand(enactResult(false))
        val error: ResponseResult<CarelevoUseCaseResponse> = ResponseResult.Error(IllegalStateException("delete patch info is failed"))
        whenever(patchForceDiscardUseCase.execute()).thenReturn(Single.just(error))
        val events = collectEvents()

        sut.startDiscardProcess()

        assertThat(events).contains(CarelevoConnectSafetyCheckEvent.DiscardFailed)
        assertThat(sut.uiState.value).isEqualTo(UiState.Idle)
        verify(carelevoPatch, never()).discardTeardown()
    }

    @Test
    fun `a force-discard that returns Failure reports failure`() {
        givenPatchState(PatchState.ConnectedBooted)
        stubCustomCommand(enactResult(false))
        val failure: ResponseResult<CarelevoUseCaseResponse> = ResponseResult.Failure("nope")
        whenever(patchForceDiscardUseCase.execute()).thenReturn(Single.just(failure))
        val events = collectEvents()

        sut.startDiscardProcess()

        assertThat(events).contains(CarelevoConnectSafetyCheckEvent.DiscardFailed)
        assertThat(sut.uiState.value).isEqualTo(UiState.Idle)
        verify(carelevoPatch, never()).discardTeardown()
    }

    @Test
    fun `a force-discard whose stream throws reports failure through doOnError`() {
        givenPatchState(PatchState.ConnectedBooted)
        stubCustomCommand(enactResult(false))
        whenever(patchForceDiscardUseCase.execute())
            .thenReturn(Single.error(RuntimeException("db down")))
        val events = collectEvents()

        sut.startDiscardProcess()

        assertThat(events).contains(CarelevoConnectSafetyCheckEvent.DiscardFailed)
        assertThat(sut.uiState.value).isEqualTo(UiState.Idle)
        verify(carelevoPatch, never()).discardTeardown()
    }

    // ---- retryAdditionalPriming ---------------------------------------------------------------

    @Test
    fun `retryAdditionalPriming is refused when bluetooth is off`() {
        whenever(carelevoPatch.isBluetoothEnabled()).thenReturn(false)
        val events = collectEvents()

        sut.retryAdditionalPriming()

        assertThat(events).contains(CarelevoConnectSafetyCheckEvent.ShowMessageBluetoothNotEnabled)
        assertThat(sut.uiState.value).isEqualTo(UiState.Idle)
        verifyBlocking(commandQueue, never()) { customCommand(any()) }
    }

    @Test
    fun `retryAdditionalPriming returns to idle without feedback on success`() {
        whenever(carelevoPatch.isBluetoothEnabled()).thenReturn(true)
        stubCustomCommand(enactResult(true))
        val events = collectEvents()

        sut.retryAdditionalPriming()

        assertThat(sut.uiState.value).isEqualTo(UiState.Idle)
        assertThat(events).doesNotContain(CarelevoConnectSafetyCheckEvent.DiscardFailed)
        verifyBlocking(commandQueue) { customCommand(any<CmdAdditionalPriming>()) }
    }

    @Test
    fun `retryAdditionalPriming surfaces feedback and returns to idle on failure`() {
        whenever(carelevoPatch.isBluetoothEnabled()).thenReturn(true)
        stubCustomCommand(enactResult(false))
        val events = collectEvents()

        sut.retryAdditionalPriming()

        assertThat(sut.uiState.value).isEqualTo(UiState.Idle)
        assertThat(events).contains(CarelevoConnectSafetyCheckEvent.DiscardFailed)
    }

    // ---- isSafetyCheckPassed ------------------------------------------------------------------

    @Test
    fun `isSafetyCheckPassed is true only when the patch recorded a passed check`() {
        givenPatchInfo(patchInfo(checkSafety = true))

        assertThat(sut.isSafetyCheckPassed()).isTrue()
    }

    @Test
    fun `isSafetyCheckPassed is false when the patch recorded a failed check`() {
        givenPatchInfo(patchInfo(checkSafety = false))

        assertThat(sut.isSafetyCheckPassed()).isFalse()
    }

    @Test
    fun `isSafetyCheckPassed is false when the check was never recorded`() {
        givenPatchInfo(patchInfo(checkSafety = null))

        assertThat(sut.isSafetyCheckPassed()).isFalse()
    }

    @Test
    fun `isSafetyCheckPassed is false when there is no patch info`() {
        givenPatchInfo(null)

        assertThat(sut.isSafetyCheckPassed()).isFalse()
    }

    // ---- isConnected --------------------------------------------------------------------------

    @Test
    fun `isConnected is false without a paired patch address`() {
        whenever(carelevoPatch.getPatchInfoAddress()).thenReturn(null)

        assertThat(sut.isConnected()).isFalse()
    }

    @Test
    fun `isConnected is false when bluetooth is off`() {
        whenever(carelevoPatch.getPatchInfoAddress()).thenReturn(address)
        whenever(carelevoPatch.isBluetoothEnabled()).thenReturn(false)

        assertThat(sut.isConnected()).isFalse()
    }

    @Test
    fun `isConnected is true when a session can be attempted`() {
        whenever(carelevoPatch.getPatchInfoAddress()).thenReturn(address)
        whenever(carelevoPatch.isBluetoothEnabled()).thenReturn(true)

        assertThat(sut.isConnected()).isTrue()
    }

    // ---- onCleared ----------------------------------------------------------------------------

    @Test
    fun `onCleared disposes the force-discard subscription without throwing`() {
        // onCleared is protected on ViewModel and the VM is final, so drive it reflectively.
        val onCleared = sut.javaClass.getDeclaredMethod("onCleared")
        onCleared.isAccessible = true

        onCleared.invoke(sut)

        assertThat(sut.uiState.value).isEqualTo(UiState.Idle)
    }
}
