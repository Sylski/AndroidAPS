package app.aaps.pump.carelevo.coordinator

import android.os.SystemClock
import app.aaps.core.data.model.BS
import app.aaps.core.data.pump.defs.PumpType
import app.aaps.core.data.time.T
import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.core.interfaces.pump.BolusProgressData
import app.aaps.core.interfaces.pump.DetailedBolusInfo
import app.aaps.core.interfaces.pump.PumpEnactResult
import app.aaps.core.interfaces.pump.PumpInsulin
import app.aaps.core.interfaces.pump.PumpRate
import app.aaps.core.interfaces.pump.PumpSync
import app.aaps.core.interfaces.resources.ResourceHelper
import app.aaps.core.interfaces.rx.AapsSchedulers
import app.aaps.core.interfaces.rx.bus.RxBus
import app.aaps.core.interfaces.utils.DateUtil
import app.aaps.core.interfaces.utils.Round
import app.aaps.core.keys.interfaces.Preferences
import app.aaps.pump.carelevo.R
import app.aaps.pump.carelevo.ble.CarelevoNewStackGateway
import app.aaps.pump.carelevo.ble.commands.BolusCancelCommand
import app.aaps.pump.carelevo.ble.commands.ExtendBolusCancelCommand
import app.aaps.pump.carelevo.ble.commands.ExtendBolusCommand
import app.aaps.pump.carelevo.ble.commands.ImmediateBolusCommand
import app.aaps.pump.carelevo.common.CarelevoPatch
import app.aaps.pump.carelevo.common.keys.CarelevoBooleanPreferenceKey
import app.aaps.pump.carelevo.domain.model.ResponseResult
import app.aaps.pump.carelevo.domain.usecase.bolus.CarelevoCancelExtendBolusInfusionUseCase
import app.aaps.pump.carelevo.domain.usecase.bolus.CarelevoCancelImmeBolusInfusionUseCase
import app.aaps.pump.carelevo.domain.usecase.bolus.CarelevoFinishImmeBolusInfusionUseCase
import app.aaps.pump.carelevo.domain.usecase.bolus.CarelevoStartExtendBolusInfusionUseCase
import app.aaps.pump.carelevo.domain.usecase.bolus.CarelevoStartImmeBolusInfusionUseCase
import app.aaps.pump.carelevo.domain.usecase.bolus.model.CancelBolusInfusionResponseModel
import app.aaps.pump.carelevo.domain.usecase.bolus.model.StartExtendBolusInfusionRequestModel
import app.aaps.pump.carelevo.domain.usecase.bolus.model.StartImmeBolusInfusionRequestModel
import app.aaps.pump.carelevo.domain.usecase.bolus.model.StartImmeBolusInfusionResponseModel
import io.reactivex.rxjava3.disposables.CompositeDisposable
import io.reactivex.rxjava3.kotlin.plusAssign
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.runBlocking
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import javax.inject.Inject
import javax.inject.Provider
import javax.inject.Singleton
import kotlin.jvm.optionals.getOrNull
import kotlin.math.min
import kotlin.math.roundToInt

@Singleton
class CarelevoBolusCoordinator @Inject constructor(
    private val aapsLogger: AAPSLogger,
    private val rh: ResourceHelper,
    private val dateUtil: DateUtil,
    private val bolusProgressData: BolusProgressData,
    private val pumpSync: PumpSync,
    private val rxBus: RxBus,
    private val aapsSchedulers: AapsSchedulers,
    private val pumpEnactResultProvider: Provider<PumpEnactResult>,
    private val carelevoPatch: CarelevoPatch,
    private val preferences: Preferences,
    private val gateway: CarelevoNewStackGateway,
    private val startImmeBolusInfusionUseCase: CarelevoStartImmeBolusInfusionUseCase,
    private val finishImmeBolusInfusionUseCase: CarelevoFinishImmeBolusInfusionUseCase,
    private val cancelImmeBolusInfusionUseCase: CarelevoCancelImmeBolusInfusionUseCase,
    private val startExtendBolusInfusionUseCase: CarelevoStartExtendBolusInfusionUseCase,
    private val cancelExtendBolusInfusionUseCase: CarelevoCancelExtendBolusInfusionUseCase
) {

    companion object {

        private const val STOP_BOLUS_TIME_OUT = 15_000L
        private const val PROGRESS_POLL_MS = 100L
        private const val RESULT_SUCCESS = 0

        // Option A guard: max time the queue worker stays blocked after a STOPPED new-stack bolus, waiting for
        // the out-of-band cancel session to confirm before it can re-dial legacy (avoids a two-GATT collision).
        // A reachable patch settles in ~1-2 s; the bound only trips for an unreachable pump (self-limiting).
        private const val CANCEL_WAIT_TIMEOUT_MS = 20_000L
    }

    @Volatile private var isImmeBolusStop = false
    private var bolusExpectMs: Long = 0
    private val _lastBolusTime = MutableStateFlow<Long?>(null)
    val lastBolusTime: StateFlow<Long?> = _lastBolusTime

    private val _lastBolusAmount = MutableStateFlow<PumpInsulin?>(null)
    val lastBolusAmount: StateFlow<PumpInsulin?> = _lastBolusAmount

    fun deliverTreatment(
        detailedBolusInfo: DetailedBolusInfo,
        serialNumber: String,
        onLastDataUpdated: () -> Unit,
        pluginDisposable: CompositeDisposable
    ): PumpEnactResult {
        aapsLogger.debug(LTag.PUMPCOMM, "deliverTreatment.start bolusType=${detailedBolusInfo.bolusType}")
        require(detailedBolusInfo.carbs == 0.0) { detailedBolusInfo.toString() }
        require(detailedBolusInfo.insulin > 0) { detailedBolusInfo.toString() }

        val result = pumpEnactResultProvider.get()
        if (!carelevoPatch.isBluetoothEnabled()) {
            return result
        }
        if (!carelevoPatch.isCarelevoConnected()) {
            return result
        }

        val infusionInfo = carelevoPatch.infusionInfo.value?.getOrNull()
        aapsLogger.warn(
            LTag.PUMPCOMM,
            "deliverTreatment.gate type=${detailedBolusInfo.bolusType}, " +
                "immeInfo=${infusionInfo?.immeBolusInfusionInfo}"
        )
        if (infusionInfo?.immeBolusInfusionInfo != null) {
            aapsLogger.warn(LTag.PUMPCOMM, "deliverTreatment.reject reason=immeBolusInProgress")
            result.success = false
            result.enacted = false
            result.bolusDelivered = 0.0
            result.comment("Another bolus is in progress")
            return result
        }

        isImmeBolusStop = false
        val actionId = (carelevoPatch.patchInfo.value?.getOrNull()?.bolusActionSeq ?: 0) + 1
        val normalizedActionId = if (actionId <= 0) 1 else ((actionId - 1) % 255) + 1

        if (preferences.get(CarelevoBooleanPreferenceKey.CARELEVO_USE_NEW_BLE_STACK)) {
            return deliverTreatmentViaNewStack(detailedBolusInfo, normalizedActionId, result, serialNumber, onLastDataUpdated, pluginDisposable)
        }

        return try {
            startImmeBolusInfusionUseCase.execute(
                StartImmeBolusInfusionRequestModel(
                    actionSeq = normalizedActionId,
                    volume = detailedBolusInfo.insulin
                )
            )
                .timeout(30, TimeUnit.SECONDS)
                .observeOn(aapsSchedulers.io)
                .subscribeOn(aapsSchedulers.io)
                .doOnSuccess { response -> handleBolusSuccess(response, detailedBolusInfo, result, serialNumber, onLastDataUpdated, pluginDisposable) }
                .doOnError { e -> handleBolusError(e, result) }
                .map { result }
                .blockingGet()
        } catch (e: Throwable) {
            aapsLogger.error(LTag.PUMPCOMM, "deliverTreatment.exception error=$e")
            result.success = false
            result.enacted = false
            result.bolusDelivered = 0.0
            result
        }
    }

    fun cancelImmediateBolus(
        serialNumber: String,
        onLastDataUpdated: () -> Unit,
        pluginDisposable: CompositeDisposable
    ) {
        if (preferences.get(CarelevoBooleanPreferenceKey.CARELEVO_USE_NEW_BLE_STACK)) {
            cancelImmediateBolusViaNewStack(serialNumber, onLastDataUpdated)
            return
        }
        val maxRetry = calculateMaxRetry(totalAllowedMs = bolusExpectMs)
        stopBolusDeliveringInternal(retryCount = 0, maxRetry = maxRetry, serialNumber = serialNumber, onLastDataUpdated = onLastDataUpdated, pluginDisposable = pluginDisposable)
    }

    fun setExtendedBolus(
        insulin: Double,
        durationInMinutes: Int,
        serialNumber: String
    ): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        if (!carelevoPatch.isBluetoothEnabled()) return result
        if (!carelevoPatch.isCarelevoConnected()) return result
        if (preferences.get(CarelevoBooleanPreferenceKey.CARELEVO_USE_NEW_BLE_STACK)) {
            return setExtendedBolusViaNewStack(insulin, durationInMinutes, serialNumber)
        }

        val response = startExtendBolusInfusionUseCase.execute(
            StartExtendBolusInfusionRequestModel(
                volume = insulin,
                minutes = durationInMinutes
            )
        ).subscribeOn(aapsSchedulers.io)
            .timeout(3000L, TimeUnit.MILLISECONDS)
            .onErrorReturn { e ->
                aapsLogger.error(LTag.PUMPCOMM, "setExtendedBolus.error", e)
                ResponseResult.Error(e)
            }
            .blockingGet()

        return when (response) {
            is ResponseResult.Success -> {
                runBlocking {
                    pumpSync.syncExtendedBolusWithPumpId(
                        timestamp = dateUtil.now(),
                        rate = PumpRate(insulin),
                        duration = T.mins(durationInMinutes.toLong()).msecs(),
                        isEmulatingTB = false,
                        pumpId = dateUtil.now(),
                        pumpType = PumpType.CAREMEDI_CARELEVO,
                        pumpSerial = serialNumber
                    )
                }

                result.success = true
                result.enacted = true
                result
            }

            else                      -> {
                result.success = false
                result.enacted = false
                result
            }
        }
    }

    fun cancelExtendedBolus(
        serialNumber: String,
        onLastDataUpdated: () -> Unit
    ): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        if (!carelevoPatch.isBluetoothEnabled()) return result
        if (!carelevoPatch.isCarelevoConnected()) return result
        if (preferences.get(CarelevoBooleanPreferenceKey.CARELEVO_USE_NEW_BLE_STACK)) {
            return cancelExtendedBolusViaNewStack(serialNumber, onLastDataUpdated)
        }

        val response = cancelExtendBolusInfusionUseCase.execute()
            .subscribeOn(aapsSchedulers.io)
            .timeout(3000L, TimeUnit.MILLISECONDS)
            .onErrorReturn { e ->
                aapsLogger.error(LTag.PUMPCOMM, "cancelExtendedBolus.error", e)
                ResponseResult.Error(e)
            }
            .blockingGet()

        return when (response) {
            is ResponseResult.Success -> {
                aapsLogger.debug(LTag.PUMPCOMM, "cancelExtendedBolus.success")
                onLastDataUpdated()
                runBlocking {
                    pumpSync.syncStopExtendedBolusWithPumpId(
                        timestamp = dateUtil.now(),
                        endPumpId = dateUtil.now(),
                        pumpType = PumpType.CAREMEDI_CARELEVO,
                        pumpSerial = serialNumber
                    )
                }

                result.success = true
                result.enacted = true
                result.isTempCancel = true
                result
            }

            else                      -> {
                result.success = false
                result.enacted = false
                result
            }
        }
    }

    /**
     * Phase-2 set-extended-bolus over the new stack (flag-gated, **delivery-critical**). Discrete
     * `ExtendBolusCommand` (0x25→0x85) via the border gateway — **`immediateDose` is always 0.0** (pure
     * extended, matching legacy; a non-zero value injects an unintended upfront dose). On `resultCode==0`
     * reuse the use case's `mode=5` persist → then the same `pumpSync.syncExtendedBolusWithPumpId`. Mirrors
     * [setExtendedBolus].
     */
    private fun setExtendedBolusViaNewStack(
        insulin: Double,
        durationInMinutes: Int,
        serialNumber: String
    ): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        val address = carelevoPatch.getPatchInfoAddress()
            ?: return result.success(false).enacted(false).comment("no patch address")
        val hour = durationInMinutes / 60
        val min = durationInMinutes % 60
        val speed = insulin / (durationInMinutes.toDouble() / 60)
        return try {
            val response = runBlocking { gateway.runSingle(address, ExtendBolusCommand(immediateDose = 0.0, extendedSpeed = speed, hour = hour, min = min)) }
            val success = response.resultCode == RESULT_SUCCESS
            val persisted = success && startExtendBolusInfusionUseCase.persistExtendBolusStarted(volume = insulin, speed = speed, minutes = durationInMinutes)
            aapsLogger.info(LTag.PUMPCOMM, "newBle.setExtendedBolus insulin=$insulin result=${response.resultCode} persisted=$persisted")
            if (success && persisted) {
                runBlocking {
                    pumpSync.syncExtendedBolusWithPumpId(
                        timestamp = dateUtil.now(),
                        rate = PumpRate(insulin),
                        duration = T.mins(durationInMinutes.toLong()).msecs(),
                        isEmulatingTB = false,
                        pumpId = dateUtil.now(),
                        pumpType = PumpType.CAREMEDI_CARELEVO,
                        pumpSerial = serialNumber
                    )
                }
                result.success = true
                result.enacted = true
                result
            } else {
                result.success = false
                result.enacted = false
                result
            }
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "newBle.setExtendedBolus FAILED", e)
            result.success = false
            result.enacted = false
            result
        }
    }

    /**
     * Phase-2 cancel-extended-bolus over the new stack (flag-gated). Discrete `ExtendBolusCancelCommand`
     * (0x29→0x89, response carries `infusedAmount`) via the border gateway → delete + recompute-mode persist →
     * `pumpSync.syncStopExtendedBolusWithPumpId`. `infusedAmount` is logged only (legacy does not reconcile it
     * into the DB). Mirrors [cancelExtendedBolus].
     */
    private fun cancelExtendedBolusViaNewStack(
        serialNumber: String,
        onLastDataUpdated: () -> Unit
    ): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        val address = carelevoPatch.getPatchInfoAddress()
            ?: return result.success(false).enacted(false).comment("no patch address")
        return try {
            val response = runBlocking { gateway.runSingle(address, ExtendBolusCancelCommand()) }
            val success = response.resultCode == RESULT_SUCCESS
            val persisted = success && cancelExtendBolusInfusionUseCase.persistExtendBolusCancelled()
            aapsLogger.info(LTag.PUMPCOMM, "newBle.cancelExtendedBolus result=${response.resultCode} infused=${response.infusedAmount} persisted=$persisted")
            if (success && persisted) {
                onLastDataUpdated()
                runBlocking {
                    pumpSync.syncStopExtendedBolusWithPumpId(
                        timestamp = dateUtil.now(),
                        endPumpId = dateUtil.now(),
                        pumpType = PumpType.CAREMEDI_CARELEVO,
                        pumpSerial = serialNumber
                    )
                }
                result.success = true
                result.enacted = true
                result.isTempCancel = true
                result
            } else {
                result.success = false
                result.enacted = false
                result
            }
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "newBle.cancelExtendedBolus FAILED", e)
            result.success = false
            result.enacted = false
            result
        }
    }

    /**
     * Phase-2 immediate bolus over the new stack (flag-gated, **delivery-critical**, Option A). Discrete
     * `ImmediateBolusCommand` (0x24→0x84, `actionId` echoed as a stricter correlation guard) via the border
     * gateway; on `resultCode==0` persist the start (`mode=3`) then reuse the EXISTING synthetic progress loop
     * ([handleBolusSuccess]) with a synthesized success response — the pump sends no progress/completion
     * frames, so the loop needs no BLE and runs after the start session has already closed. Mirrors
     * [deliverTreatment].
     */
    private fun deliverTreatmentViaNewStack(
        detailedBolusInfo: DetailedBolusInfo,
        actionId: Int,
        result: PumpEnactResult,
        serialNumber: String,
        onLastDataUpdated: () -> Unit,
        pluginDisposable: CompositeDisposable
    ): PumpEnactResult {
        val address = carelevoPatch.getPatchInfoAddress()
        if (address == null) {
            result.success = false
            result.enacted = false
            result.bolusDelivered = 0.0
            return result.comment("no patch address")
        }
        return try {
            val response = runBlocking { gateway.runSingle(address, ImmediateBolusCommand(actionId, detailedBolusInfo.insulin)) }
            if (response.resultCode != RESULT_SUCCESS) {
                aapsLogger.error(LTag.PUMPCOMM, "newBle.deliverTreatment start failed result=${response.resultCode}")
                result.success = false
                result.enacted = false
                result.bolusDelivered = 0.0
                return result.comment(rh.gs(R.string.alarm_feat_msg_check_patch_connect))
            }
            if (!startImmeBolusInfusionUseCase.persistImmeBolusStarted(actionId, detailedBolusInfo.insulin, response.expectedCompletionSeconds)) {
                aapsLogger.error(LTag.PUMPCOMM, "newBle.deliverTreatment persist failed")
                result.success = false
                result.enacted = false
                result.bolusDelivered = 0.0
                return result.comment("Internal error")
            }
            aapsLogger.info(LTag.PUMPCOMM, "newBle.deliverTreatment start insulin=${detailedBolusInfo.insulin} result=${response.resultCode} expectSec=${response.expectedCompletionSeconds}")
            // Start session has closed; run the synthetic progress loop with NO link (Option A) by reusing the
            // exact legacy loop via handleBolusSuccess with a synthesized success response.
            handleBolusSuccess(
                ResponseResult.Success(StartImmeBolusInfusionResponseModel(expectSec = response.expectedCompletionSeconds)),
                detailedBolusInfo,
                result,
                serialNumber,
                onLastDataUpdated,
                pluginDisposable
            )
            awaitImmeBolusCancelIfStopped()
            result
        } catch (e: Exception) {
            handleBolusError(e, result)
            result
        }
    }

    /**
     * Option A concurrency guard: if the user stopped the bolus, keep the queue worker blocked here until the
     * OUT-OF-BAND cancel session has confirmed ([isImmeBolusStop]) — otherwise the freed worker could re-dial
     * the legacy GATT while the cancel's new-transport session is still open (two GATTs → status-133). Bounded
     * by [CANCEL_WAIT_TIMEOUT_MS] so an unreachable-pump cancel can't hang the worker; a reachable patch
     * settles in ~1-2 s.
     */
    private fun awaitImmeBolusCancelIfStopped() {
        if (!bolusProgressData.isStopPressed || isImmeBolusStop) return
        val deadline = System.currentTimeMillis() + CANCEL_WAIT_TIMEOUT_MS
        while (!isImmeBolusStop && System.currentTimeMillis() < deadline) {
            SystemClock.sleep(PROGRESS_POLL_MS)
        }
        aapsLogger.info(LTag.PUMPCOMM, "newBle.deliverTreatment stop settled isImmeBolusStop=$isImmeBolusStop")
    }

    /**
     * Phase-2 immediate-bolus cancel over the new stack (flag-gated, Option A). Stop arrives OUT-OF-BAND (off
     * the queue worker, via `cancelAllBoluses`) while the delivery loop runs with no link, so this opens a
     * FRESH cancel session — the session mutex serializes it against any in-flight session (the start session
     * has already closed). Discrete `BolusCancelCommand` (0x2C→0x8C); on success record the pump-reported
     * partial `infusedAmount` ([PumpSync]) + delete/recompute persist + set [isImmeBolusStop] (which breaks the
     * delivery loop). Retries a timed-out cancel up to a bolus-duration-derived bound. Mirrors
     * [stopBolusDeliveringInternal].
     */
    private fun cancelImmediateBolusViaNewStack(serialNumber: String, onLastDataUpdated: () -> Unit) {
        val address = carelevoPatch.getPatchInfoAddress()
        if (address == null) {
            aapsLogger.error(LTag.PUMPCOMM, "newBle.cancelImmediateBolus no patch address")
            return
        }
        val maxRetry = calculateMaxRetry(totalAllowedMs = bolusExpectMs).coerceAtLeast(0)
        var attempt = 0
        while (attempt <= maxRetry && !isImmeBolusStop) {
            try {
                val response = runBlocking { gateway.runSingle(address, BolusCancelCommand()) }
                if (response.resultCode == RESULT_SUCCESS) {
                    onLastDataUpdated()
                    val infusedAmount = response.infusedAmount
                    bolusProgressData.updateProgress(
                        bolusProgressData.state.value?.percent ?: 100,
                        rh.gs(app.aaps.core.interfaces.R.string.bolus_delivered_successfully, infusedAmount.toFloat()),
                        PumpInsulin(infusedAmount)
                    )
                    cancelImmeBolusInfusionUseCase.persistImmeBolusCancelled()
                    runBlocking {
                        pumpSync.syncBolusWithPumpId(
                            dateUtil.now(),
                            PumpInsulin(infusedAmount),
                            BS.Type.NORMAL,
                            dateUtil.now(),
                            PumpType.CAREMEDI_CARELEVO,
                            serialNumber
                        )
                    }
                    isImmeBolusStop = true
                    aapsLogger.info(LTag.PUMPCOMM, "newBle.cancelImmediateBolus infused=$infusedAmount")
                    return
                }
                aapsLogger.error(LTag.PUMPCOMM, "newBle.cancelImmediateBolus failed result=${response.resultCode}")
                return
            } catch (e: Exception) {
                aapsLogger.error(LTag.PUMPCOMM, "newBle.cancelImmediateBolus attempt=$attempt error=$e")
                attempt++
            }
        }
        if (!isImmeBolusStop) {
            aapsLogger.error(LTag.PUMPCOMM, "newBle.cancelImmediateBolus exhausted maxRetry=$maxRetry")
        }
    }

    private fun handleBolusSuccess(
        response: ResponseResult<*>,
        detailedInfo: DetailedBolusInfo,
        result: PumpEnactResult,
        serialNumber: String,
        onLastDataUpdated: () -> Unit,
        pluginDisposable: CompositeDisposable
    ) {
        if (response !is ResponseResult.Success) {
            val message = when (response) {
                is ResponseResult.Failure -> response.message
                is ResponseResult.Error   -> response.e.message ?: response.e.toString()
                else                      -> "Unknown bolus response"
            }
            aapsLogger.error(LTag.PUMPCOMM, "deliverTreatment.nonSuccess response=$response")
            result.success = false
            result.enacted = false
            result.bolusDelivered = 0.0
            result.comment(message)
            return
        }

        val data = response.data as StartImmeBolusInfusionResponseModel

        val now = System.currentTimeMillis()
        onLastDataUpdated()
        _lastBolusTime.value = now
        _lastBolusAmount.value = PumpInsulin(detailedInfo.insulin)

        val stepUnit = 0.05
        val totalInsulin = detailedInfo.insulin
        val totalSteps = (Round.ceilTo(totalInsulin, stepUnit) / stepUnit).roundToInt().coerceAtLeast(1)

        bolusExpectMs = data.expectSec * 1000L
        val delayMs = bolusExpectMs / totalSteps

        var completed = false
        for (step in 0..totalSteps) {
            // Cancellation-cooperative: break promptly when the user presses stop.
            // isStopPressed is the core flag flipped the instant the user taps stop
            // (CommandQueue.cancelAllBoluses), while isImmeBolusStop is only set later once the
            // pump confirms the cancel. Poll both so the progress loop stops advancing immediately
            // instead of only after the pump round-trip.
            if (isImmeBolusStop || bolusProgressData.isStopPressed) break

            if (step == totalSteps) {
                bolusProgressData.updateProgress(
                    100,
                    rh.gs(
                        app.aaps.core.interfaces.R.string.bolus_delivered_successfully,
                        detailedInfo.insulin.toFloat()
                    ),
                    PumpInsulin(detailedInfo.insulin)
                )
                runBlocking {
                    pumpSync.syncBolusWithPumpId(
                        detailedInfo.timestamp,
                        PumpInsulin(detailedInfo.insulin),
                        detailedInfo.bolusType,
                        dateUtil.now(),
                        PumpType.CAREMEDI_CARELEVO,
                        serialNumber
                    )
                }
                handleFinishImmeBolus(onLastDataUpdated, pluginDisposable)
                completed = true
            } else {
                // Sleep the per-step delay in short slices so a mid-step stop is honored within
                // PROGRESS_POLL_MS rather than after the full delayMs (SystemClock.sleep is blocking
                // and does not observe coroutine cancellation).
                var slept = 0L
                while (slept < delayMs && !isImmeBolusStop && !bolusProgressData.isStopPressed) {
                    val chunk = min(PROGRESS_POLL_MS, delayMs - slept)
                    SystemClock.sleep(chunk)
                    slept += chunk
                }
                if (isImmeBolusStop || bolusProgressData.isStopPressed) break
                val delivering = min(step * stepUnit, detailedInfo.insulin)
                val percent = if (totalInsulin <= 0.0) 0 else ((delivering / totalInsulin) * 100).toInt()
                bolusProgressData.updateProgress(
                    percent,
                    rh.gs(app.aaps.core.interfaces.R.string.bolus_delivering, delivering),
                    PumpInsulin(delivering)
                )
            }
        }

        if (completed) {
            result.success = true
            result.enacted = true
            result.bolusDelivered = detailedInfo.insulin
        } else {
            // User stopped mid-bolus (loop broke before the finish step). The actual partial amount is
            // recorded separately by stopBolusDeliveringInternal via syncBolusWithPumpId(infusedAmount),
            // so don't report the full requested dose as delivered. Mirror VirtualPumpPlugin's stop contract.
            result.success = false
            result.enacted = false
            result.bolusDelivered = 0.0
            result.comment(rh.gs(app.aaps.core.ui.R.string.stop))
        }
    }

    private fun handleBolusError(e: Throwable, result: PumpEnactResult) {
        aapsLogger.error(LTag.PUMPCOMM, "deliverTreatment.error error=$e")
        result.success = false
        result.enacted = false
        result.bolusDelivered = 0.0
        if (e is TimeoutException) {
            result.comment(rh.gs(R.string.alarm_feat_msg_check_patch_connect))
        }
    }

    private fun handleFinishImmeBolus(
        onLastDataUpdated: () -> Unit,
        pluginDisposable: CompositeDisposable
    ) {
        pluginDisposable += finishImmeBolusInfusionUseCase.execute()
            .observeOn(aapsSchedulers.main)
            .subscribeOn(aapsSchedulers.io)
            .timeout(3000L, TimeUnit.MILLISECONDS)
            .subscribe(
                { response ->
                    when (response) {
                        is ResponseResult.Success -> {
                            onLastDataUpdated()
                            aapsLogger.debug(LTag.PUMPCOMM, "finishImmeBolus.success")
                        }

                        is ResponseResult.Error   -> {
                            aapsLogger.error(LTag.PUMPCOMM, "finishImmeBolus.responseError error=${response.e}")
                        }

                        else                      -> {
                            aapsLogger.error(LTag.PUMPCOMM, "finishImmeBolus.failure")
                        }
                    }
                },
                { e ->
                    aapsLogger.error(LTag.PUMPCOMM, "finishImmeBolus.subscribeError error=$e")
                }
            )
    }

    private fun calculateMaxRetry(
        totalAllowedMs: Long,
        timeoutMs: Long = STOP_BOLUS_TIME_OUT
    ): Int {
        aapsLogger.debug(LTag.PUMPCOMM, "stopBolus.calculateMaxRetry totalAllowedMs=$totalAllowedMs timeoutMs=$timeoutMs")
        if (timeoutMs == 0L) {
            return 3
        }
        return ((totalAllowedMs + timeoutMs - 1) / timeoutMs).toInt() - 1
    }

    private fun stopBolusDeliveringInternal(
        retryCount: Int,
        maxRetry: Int = 3,
        serialNumber: String,
        onLastDataUpdated: () -> Unit,
        pluginDisposable: CompositeDisposable
    ) {
        aapsLogger.debug(
            LTag.PUMPCOMM,
            "stopBolus.start retry=$retryCount maxRetry=$maxRetry"
        )

        pluginDisposable += cancelImmeBolusInfusionUseCase.execute()
            .subscribeOn(aapsSchedulers.io)
            .observeOn(aapsSchedulers.io)
            .timeout(STOP_BOLUS_TIME_OUT, TimeUnit.MILLISECONDS)
            .subscribe(
                { response ->
                    when (response) {
                        is ResponseResult.Success -> {
                            onLastDataUpdated()
                            val cancelResult = response.data as CancelBolusInfusionResponseModel
                            aapsLogger.debug(LTag.PUMPCOMM, "stopBolus.success result=$cancelResult")
                            bolusProgressData.updateProgress(
                                bolusProgressData.state.value?.percent ?: 100,
                                rh.gs(
                                    app.aaps.core.interfaces.R.string.bolus_delivered_successfully,
                                    cancelResult.infusedAmount.toFloat()
                                ),
                                PumpInsulin(cancelResult.infusedAmount)
                            )
                            runBlocking {
                                pumpSync.syncBolusWithPumpId(
                                    dateUtil.now(),
                                    PumpInsulin(cancelResult.infusedAmount),
                                    BS.Type.NORMAL,
                                    dateUtil.now(),
                                    PumpType.CAREMEDI_CARELEVO,
                                    serialNumber
                                )
                            }
                            isImmeBolusStop = true
                        }

                        is ResponseResult.Error   -> {
                            aapsLogger.error(LTag.PUMPCOMM, "stopBolus.responseError error=${response.e}")
                        }

                        else                      -> {
                            aapsLogger.error(LTag.PUMPCOMM, "stopBolus.failure")
                        }
                    }
                },
                { throwable ->
                    if (throwable is TimeoutException) {
                        aapsLogger.error(LTag.PUMPCOMM, "stopBolus.timeout timeoutMs=$STOP_BOLUS_TIME_OUT retry=$retryCount")
                        if (retryCount < maxRetry) {
                            stopBolusDeliveringInternal(
                                retryCount = retryCount + 1,
                                maxRetry = maxRetry,
                                serialNumber = serialNumber,
                                onLastDataUpdated = onLastDataUpdated,
                                pluginDisposable = pluginDisposable
                            )
                        } else {
                            aapsLogger.error(LTag.PUMPCOMM, "stopBolus.timeout.exhausted maxRetry=$maxRetry")
                        }
                    } else {
                        aapsLogger.error(LTag.PUMPCOMM, "stopBolus.error error=$throwable")
                    }
                }
            )
    }
}
