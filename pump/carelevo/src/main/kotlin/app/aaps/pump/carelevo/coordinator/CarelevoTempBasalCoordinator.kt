package app.aaps.pump.carelevo.coordinator

import app.aaps.core.data.pump.defs.PumpType
import app.aaps.core.data.time.T
import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.core.interfaces.pump.PumpEnactResult
import app.aaps.core.interfaces.pump.PumpRate
import app.aaps.core.interfaces.pump.PumpSync
import app.aaps.core.interfaces.rx.AapsSchedulers
import app.aaps.core.interfaces.utils.DateUtil
import app.aaps.core.keys.interfaces.Preferences
import app.aaps.pump.carelevo.ble.CarelevoNewStackGateway
import app.aaps.pump.carelevo.ble.commands.TempBasalCancelCommand
import app.aaps.pump.carelevo.ble.commands.TempBasalCommand
import app.aaps.pump.carelevo.common.CarelevoPatch
import app.aaps.pump.carelevo.common.keys.CarelevoBooleanPreferenceKey
import app.aaps.pump.carelevo.domain.model.ResponseResult
import app.aaps.pump.carelevo.domain.usecase.basal.CarelevoCancelTempBasalInfusionUseCase
import app.aaps.pump.carelevo.domain.usecase.basal.CarelevoStartTempBasalInfusionUseCase
import app.aaps.pump.carelevo.domain.usecase.basal.model.StartTempBasalInfusionRequestModel
import kotlinx.coroutines.runBlocking
import java.util.concurrent.TimeUnit
import javax.inject.Inject
import javax.inject.Provider
import javax.inject.Singleton

@Singleton
class CarelevoTempBasalCoordinator @Inject constructor(
    private val aapsLogger: AAPSLogger,
    private val aapsSchedulers: AapsSchedulers,
    private val dateUtil: DateUtil,
    private val pumpSync: PumpSync,
    private val pumpEnactResultProvider: Provider<PumpEnactResult>,
    private val carelevoPatch: CarelevoPatch,
    private val preferences: Preferences,
    private val gateway: CarelevoNewStackGateway,
    private val startTempBasalInfusionUseCase: CarelevoStartTempBasalInfusionUseCase,
    private val cancelTempBasalInfusionUseCase: CarelevoCancelTempBasalInfusionUseCase
) {

    private companion object {

        private const val RESULT_SUCCESS = 0
    }

    fun setTempBasalAbsolute(
        absoluteRate: Double,
        durationInMinutes: Int,
        tbrType: PumpSync.TemporaryBasalType,
        serialNumber: String,
        onLastDataUpdated: () -> Unit
    ): PumpEnactResult {
        aapsLogger.info(
            LTag.PUMPCOMM,
            "setTempBasalAbsolute.start absoluteRate=${absoluteRate.toFloat()} durationInMinutes=${durationInMinutes.toLong()}"
        )
        val result = pumpEnactResultProvider.get()
        if (!carelevoPatch.isBluetoothEnabled()) {
            aapsLogger.info(LTag.PUMPCOMM, "setTempBasalAbsolute.skip reason=bluetoothDisabled")
            return result
        }
        if (!carelevoPatch.isCarelevoConnected()) {
            aapsLogger.info(LTag.PUMPCOMM, "setTempBasalAbsolute.skip reason=notConnected")
            return result
        }
        if (preferences.get(CarelevoBooleanPreferenceKey.CARELEVO_USE_NEW_BLE_STACK)) {
            return setTempBasalAbsoluteViaNewStack(absoluteRate, durationInMinutes, tbrType, serialNumber, onLastDataUpdated)
        }

        val response = startTempBasalInfusionUseCase.execute(
            StartTempBasalInfusionRequestModel(
                isUnit = true,
                speed = absoluteRate,
                minutes = durationInMinutes
            )
        )
            .subscribeOn(aapsSchedulers.io)
            .timeout(10, TimeUnit.SECONDS)
            .onErrorReturn { throwable ->
                aapsLogger.error(LTag.PUMPCOMM, "setTempBasalAbsolute.error", throwable)
                ResponseResult.Error(throwable)
            }
            .blockingGet()

        return when (response) {
            is ResponseResult.Success -> {
                aapsLogger.debug(LTag.PUMPCOMM, "setTempBasalAbsolute.success")
                onLastDataUpdated()
                runBlocking {
                    pumpSync.syncTemporaryBasalWithPumpId(
                        timestamp = dateUtil.now(),
                        rate = PumpRate(absoluteRate),
                        duration = T.mins(durationInMinutes.toLong()).msecs(),
                        isAbsolute = true,
                        type = tbrType,
                        pumpId = dateUtil.now(),
                        pumpType = PumpType.CAREMEDI_CARELEVO,
                        pumpSerial = serialNumber
                    )
                }

                result.success(true).enacted(true)
                    .duration(durationInMinutes)
                    .absolute(absoluteRate)
                    .isPercent(false)
                    .isTempCancel(false)
            }

            else                      -> {
                aapsLogger.error(LTag.PUMPCOMM, "setTempBasalAbsolute.failure response=$response")
                result.success(false).enacted(false).comment("Internal error")
            }
        }
    }

    fun setTempBasalPercent(
        percent: Int,
        durationInMinutes: Int,
        tbrType: PumpSync.TemporaryBasalType,
        serialNumber: String,
        onLastDataUpdated: () -> Unit
    ): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        aapsLogger.debug(LTag.PUMPCOMM, "setTempBasalPercent.start percent=$percent durationInMinutes=$durationInMinutes")
        if (!carelevoPatch.isBluetoothEnabled()) {
            aapsLogger.debug(LTag.PUMPCOMM, "setTempBasalPercent.skip reason=bluetoothDisabled")
            return result
        }
        if (!carelevoPatch.isCarelevoConnected()) {
            aapsLogger.debug(LTag.PUMPCOMM, "setTempBasalPercent.skip reason=notConnected")
            return result
        }
        if (preferences.get(CarelevoBooleanPreferenceKey.CARELEVO_USE_NEW_BLE_STACK)) {
            return setTempBasalPercentViaNewStack(percent, durationInMinutes, tbrType, serialNumber, onLastDataUpdated)
        }

        return startTempBasalInfusionUseCase.execute(
            StartTempBasalInfusionRequestModel(
                isUnit = false,
                percent = percent,
                minutes = durationInMinutes
            )
        )
            .observeOn(aapsSchedulers.io)
            .subscribeOn(aapsSchedulers.io)
            .timeout(3000L, TimeUnit.MILLISECONDS)
            .doOnSuccess { response ->
                when (response) {
                    is ResponseResult.Success -> {
                        aapsLogger.debug(LTag.PUMPCOMM, "setTempBasalPercent.success")
                        onLastDataUpdated()
                        runBlocking {
                            pumpSync.syncTemporaryBasalWithPumpId(
                                timestamp = dateUtil.now(),
                                rate = PumpRate(percent.toDouble()),
                                duration = T.mins(durationInMinutes.toLong()).msecs(),
                                isAbsolute = false,
                                type = tbrType,
                                pumpId = dateUtil.now(),
                                pumpType = PumpType.CAREMEDI_CARELEVO,
                                pumpSerial = serialNumber
                            )
                        }

                        result.success = true
                        result.enacted = true
                        result.duration = durationInMinutes
                        result.percent = percent
                        result.isPercent = true
                        result.isTempCancel = false
                    }

                    is ResponseResult.Error   -> {
                        aapsLogger.error(LTag.PUMPCOMM, "setTempBasalPercent.responseError error=${response.e}")
                    }

                    else                      -> {
                        aapsLogger.error(LTag.PUMPCOMM, "setTempBasalPercent.failure")
                    }
                }
            }.doOnError {
                aapsLogger.error(LTag.PUMPCOMM, "setTempBasalPercent.error", it)
                result.success = false
                result.enacted = false
            }.map {
                result
            }.blockingGet()
    }

    fun cancelTempBasal(
        serialNumber: String,
        onLastDataUpdated: () -> Unit
    ): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        aapsLogger.debug(LTag.PUMPCOMM, "cancelTempBasal.start")
        if (!carelevoPatch.isBluetoothEnabled()) {
            aapsLogger.debug(LTag.PUMPCOMM, "cancelTempBasal.skip reason=bluetoothDisabled")
            return result
        }
        if (!carelevoPatch.isCarelevoConnected()) {
            aapsLogger.debug(LTag.PUMPCOMM, "cancelTempBasal.skip reason=notConnected")
            return result
        }
        if (preferences.get(CarelevoBooleanPreferenceKey.CARELEVO_USE_NEW_BLE_STACK)) {
            return cancelTempBasalViaNewStack(serialNumber, onLastDataUpdated)
        }

        return cancelTempBasalInfusionUseCase.execute()
            .delaySubscription(2000L, TimeUnit.MILLISECONDS)
            .subscribeOn(aapsSchedulers.io)
            .observeOn(aapsSchedulers.io)
            .timeout(15000L, TimeUnit.MILLISECONDS)
            .map { response ->
                when (response) {
                    is ResponseResult.Success -> {
                        aapsLogger.debug(LTag.PUMPCOMM, "cancelTempBasal.success")
                        onLastDataUpdated()
                        runBlocking {
                            pumpSync.syncStopTemporaryBasalWithPumpId(
                                timestamp = dateUtil.now(),
                                endPumpId = dateUtil.now(),
                                pumpType = PumpType.CAREMEDI_CARELEVO,
                                pumpSerial = serialNumber
                            )
                        }

                        result.success = true
                        result.enacted = true
                        result.isTempCancel = true
                    }

                    else                      -> {
                        aapsLogger.error(LTag.PUMPCOMM, "cancelTempBasal.failure response=$response")
                        result.success = false
                        result.enacted = false
                    }
                }
                result
            }
            .onErrorReturn { e ->
                aapsLogger.error(LTag.PUMPCOMM, "cancelTempBasal.error error=$e")
                result.success = false
                result.enacted = false
                result
            }
            .blockingGet()
    }

    /**
     * Phase-2 set-temp-basal-absolute over the new stack (flag-gated, **delivery-critical**). Discrete
     * `TempBasalCommand.byUnit` (0x23→0x83) via the border gateway → on `resultCode==0` reuse the use case's
     * `mode=2` persist → then the same `pumpSync.syncTemporaryBasalWithPumpId` the legacy path does. Mirrors
     * [setTempBasalAbsolute].
     */
    private fun setTempBasalAbsoluteViaNewStack(
        absoluteRate: Double,
        durationInMinutes: Int,
        tbrType: PumpSync.TemporaryBasalType,
        serialNumber: String,
        onLastDataUpdated: () -> Unit
    ): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        val address = carelevoPatch.getPatchInfoAddress()
            ?: return result.success(false).enacted(false).comment("no patch address")
        val hour = durationInMinutes / 60
        val min = durationInMinutes % 60
        return try {
            val response = runBlocking { gateway.runSingle(address, TempBasalCommand.byUnit(absoluteRate, hour, min)) }
            val success = response.resultCode == RESULT_SUCCESS
            val persisted = success && startTempBasalInfusionUseCase.persistTempBasalStarted(
                StartTempBasalInfusionRequestModel(isUnit = true, speed = absoluteRate, minutes = durationInMinutes)
            )
            aapsLogger.info(LTag.PUMPCOMM, "newBle.setTempBasalAbsolute rate=$absoluteRate result=${response.resultCode} persisted=$persisted")
            if (success && persisted) {
                onLastDataUpdated()
                runBlocking {
                    pumpSync.syncTemporaryBasalWithPumpId(
                        timestamp = dateUtil.now(),
                        rate = PumpRate(absoluteRate),
                        duration = T.mins(durationInMinutes.toLong()).msecs(),
                        isAbsolute = true,
                        type = tbrType,
                        pumpId = dateUtil.now(),
                        pumpType = PumpType.CAREMEDI_CARELEVO,
                        pumpSerial = serialNumber
                    )
                }
                result.success(true).enacted(true)
                    .duration(durationInMinutes)
                    .absolute(absoluteRate)
                    .isPercent(false)
                    .isTempCancel(false)
            } else {
                result.success(false).enacted(false).comment("Internal error")
            }
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "newBle.setTempBasalAbsolute FAILED", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    /**
     * Phase-2 set-temp-basal-percent over the new stack (flag-gated). Discrete `TempBasalCommand.byPercent`
     * (5-byte, value=`percent/100`) via the border gateway → `mode=2` persist → `pumpSync`. Mirrors
     * [setTempBasalPercent].
     */
    private fun setTempBasalPercentViaNewStack(
        percent: Int,
        durationInMinutes: Int,
        tbrType: PumpSync.TemporaryBasalType,
        serialNumber: String,
        onLastDataUpdated: () -> Unit
    ): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        val address = carelevoPatch.getPatchInfoAddress()
            ?: return result.success(false).enacted(false).comment("no patch address")
        val hour = durationInMinutes / 60
        val min = durationInMinutes % 60
        return try {
            val response = runBlocking { gateway.runSingle(address, TempBasalCommand.byPercent(percent, hour, min)) }
            val success = response.resultCode == RESULT_SUCCESS
            val persisted = success && startTempBasalInfusionUseCase.persistTempBasalStarted(
                StartTempBasalInfusionRequestModel(isUnit = false, percent = percent, minutes = durationInMinutes)
            )
            aapsLogger.info(LTag.PUMPCOMM, "newBle.setTempBasalPercent percent=$percent result=${response.resultCode} persisted=$persisted")
            if (success && persisted) {
                onLastDataUpdated()
                runBlocking {
                    pumpSync.syncTemporaryBasalWithPumpId(
                        timestamp = dateUtil.now(),
                        rate = PumpRate(percent.toDouble()),
                        duration = T.mins(durationInMinutes.toLong()).msecs(),
                        isAbsolute = false,
                        type = tbrType,
                        pumpId = dateUtil.now(),
                        pumpType = PumpType.CAREMEDI_CARELEVO,
                        pumpSerial = serialNumber
                    )
                }
                result.success = true
                result.enacted = true
                result.duration = durationInMinutes
                result.percent = percent
                result.isPercent = true
                result.isTempCancel = false
                result
            } else {
                result.success(false).enacted(false).comment("Internal error")
            }
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "newBle.setTempBasalPercent FAILED", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    /**
     * Phase-2 cancel-temp-basal over the new stack (flag-gated). Discrete `TempBasalCancelCommand`
     * (0x2D→0x8D) via the border gateway → delete + recompute-mode persist → `pumpSync.syncStop…`. The legacy
     * 2 s `delaySubscription` is dropped (the session's own settle covers it). Mirrors [cancelTempBasal].
     */
    private fun cancelTempBasalViaNewStack(
        serialNumber: String,
        onLastDataUpdated: () -> Unit
    ): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        val address = carelevoPatch.getPatchInfoAddress()
            ?: return result.success(false).enacted(false).comment("no patch address")
        return try {
            val response = runBlocking { gateway.runSingle(address, TempBasalCancelCommand()) }
            val success = response.resultCode == RESULT_SUCCESS
            val persisted = success && cancelTempBasalInfusionUseCase.persistTempBasalCancelled()
            aapsLogger.info(LTag.PUMPCOMM, "newBle.cancelTempBasal result=${response.resultCode} persisted=$persisted")
            if (success && persisted) {
                onLastDataUpdated()
                runBlocking {
                    pumpSync.syncStopTemporaryBasalWithPumpId(
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
            aapsLogger.error(LTag.PUMPCOMM, "newBle.cancelTempBasal FAILED", e)
            result.success = false
            result.enacted = false
            result
        }
    }

}
