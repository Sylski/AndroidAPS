package app.aaps.pump.carelevo.ble

import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.pump.carelevo.ble.commands.BasalProgramCommand
import app.aaps.pump.carelevo.ble.commands.InfusionInfoCommand
import app.aaps.pump.carelevo.ble.commands.InfusionInfoResponse
import app.aaps.pump.carelevo.ble.commands.PatchInfoCommand
import app.aaps.pump.carelevo.ble.commands.PatchInfoResponse
import app.aaps.pump.carelevo.ble.commands.SafetyCheckCommand
import app.aaps.pump.carelevo.ble.commands.SafetyCheckResponse
import app.aaps.pump.carelevo.ble.gatt.BleTransportGattConnection
import app.aaps.pump.carelevo.ble.gatt.GattConnState
import app.aaps.pump.carelevo.ble.gatt.GattEvent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filterIsInstance
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeout
import java.util.UUID
import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton

/**
 * Owns one full new-transport BLE session for the Phase-2 [BleClient] stack: connect → discover
 * services → enable notifications → run one [BleClient] exchange → close. This is the reusable
 * session backbone the wider migration builds on (see `_docs/carelevo-new-ble-stack.md`).
 *
 * Each call builds a **fresh** [BleTransportGattConnection] + [BleClientImpl] + [CoroutineScope] and
 * tears them down when done. That is deliberate: [BleTransportGattConnection.close] is one-shot (it
 * latches `closed` and releases the transport's single listener slot), so a long-lived shared
 * instance would brick after its first session. Per-session-fresh keeps each session independent.
 *
 * The new transport opens its OWN GATT, independent of the legacy `CarelevoBleMangerImpl`, so a
 * session must not run concurrently with the legacy link (two GATT clients to one patch = the
 * status-133 collision). The caller (a flag-gated customCommand) is responsible for dropping the
 * legacy link and suppressing its reconnect before invoking a session.
 */
@Singleton
class CarelevoBleSession @Inject constructor(
    private val transport: CarelevoBleTransport,
    @Named("characterRx") private val writeUuid: UUID,
    @Named("characterTx") private val notifyUuid: UUID,
    private val aapsLogger: AAPSLogger
) {

    // The new transport is a @Singleton with a SINGLE GATT + single listener slot, so two sessions can
    // never physically overlap. This mutex enforces that at the API level: every session serializes, so an
    // out-of-band caller (e.g. a bolus cancel fired off the queue worker by cancelAllBoluses) waits for the
    // in-flight session to close and release before it opens — never a two-GATT status-133 collision.
    private val sessionMutex = Mutex()

    /**
     * Connect to [address], read Patch Info (0x33 → 0x93 RPT1 + 0x94 RPT2), and close.
     *
     * @throws IllegalArgumentException if the BLE stack refuses to start the connection.
     * @throws kotlinx.coroutines.TimeoutCancellationException on connect-handshake or read timeout.
     * Always closes the connection and cancels the session scope, even on failure.
     */
    /** Read Patch Info (0x33 → 0x93 RPT1 + 0x94 RPT2). */
    suspend fun readPatchInfo(address: String): PatchInfoResponse =
        withSession(address, "patch info") { it.requestMultiple(PatchInfoCommand()) }

    /** Read Infusion Info (0x31 → 0x91) — the periodic status read (reservoir, totals, pump state). */
    suspend fun readInfusionInfo(address: String): InfusionInfoResponse =
        withSession(address, "infusion info") { it.request(InfusionInfoCommand()) }

    /** Run any single-response [command] (write or read) on a fresh new-transport session. */
    suspend fun <R : BleResponse> runSingle(address: String, command: BleCommand<R>, timeoutMs: Long = READ_TIMEOUT_MS): R =
        withSession(address, command::class.simpleName ?: "command", timeoutMs) { it.request(command) }

    /**
     * Run the streaming Safety Check (0x12 → 0x72). [onFrame] is invoked for every decoded frame (each
     * progress report and the terminal SUCCESS/error) as the pump reports it; the stream completes on the
     * terminal frame. Uses a long timeout — the check runs ~100-210 s.
     */
    suspend fun runSafetyCheck(address: String, onFrame: (SafetyCheckResponse) -> Unit) =
        withSession(address, "safety check", SAFETY_CHECK_TIMEOUT_MS) { client ->
            client.requestStream(SafetyCheckCommand()).collect { onFrame(it) }
        }

    /**
     * Set the initial basal program (activation, 0x13 → 0x73). A full program is **three sequential
     * [BasalProgramCommand]s** (seqNo 0, 1, 2) that MUST share ONE connection, so — unlike [runSingle] —
     * all three run inside a single session. [programs] are the per-seqNo segment-speed lists (v2 sends
     * speed only). Returns true only if every write reports `resultCode == 0`; short-circuits on the first
     * failure (`all` stops early) so a rejected seqNo does not send the rest of a partial program.
     */
    suspend fun runBasalProgram(address: String, programs: List<List<Double>>): Boolean =
        withSession(address, "basal program", BASAL_PROGRAM_TIMEOUT_MS) { client ->
            programs.withIndex().all { (index, speeds) ->
                client.request(BasalProgramCommand(isUpdate = false, seqNo = index, segmentSpeeds = speeds)).resultCode == RESULT_SUCCESS
            }
        }

    /**
     * Open a fresh connection, run [block] against the [BleClient], and close. Each call gets its own
     * adapter+client+scope — see the class KDoc for why (one-shot [BleTransportGattConnection.close]).
     */
    private suspend fun <R> withSession(address: String, label: String, timeoutMs: Long = READ_TIMEOUT_MS, block: suspend (BleClient) -> R): R =
        sessionMutex.withLock {
            // BluetoothAdapter.getRemoteDevice requires an UPPERCASE MAC (lowercase throws
            // IllegalArgumentException); the stored address is lowercase, so normalize here.
            val mac = address.uppercase()
            val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
            val gatt = BleTransportGattConnection(transport, writeUuid, notifyUuid, scope)
            val client: BleClient = BleClientImpl(gatt, writeUuid, notifyUuid, scope)
            try {
                open(gatt, mac)
                aapsLogger.debug(LTag.PUMPCOMM, "bleSession: reading $label")
                withTimeout(timeoutMs) { block(client) }
            } finally {
                gatt.close()
                scope.cancel()
            }
        }

    private suspend fun open(gatt: BleTransportGattConnection, address: String) = coroutineScope {
        // Subscribe to the CONNECTED event BEFORE calling connect() so the state change cannot race
        // ahead of our collector. UNDISPATCHED runs the async body up to the flow subscription
        // synchronously, guaranteeing the subscription is live before connect() fires.
        val connected = async(start = CoroutineStart.UNDISPATCHED) {
            withTimeout(CONNECT_TIMEOUT_MS) {
                gatt.events
                    .filterIsInstance<GattEvent.ConnectionStateChanged>()
                    .first { it.state == GattConnState.CONNECTED }
            }
        }
        aapsLogger.debug(LTag.PUMPCOMM, "bleSession: connecting to $address")
        require(gatt.connect(address)) { "bleSession: connect() refused for $address" }
        connected.await()
        aapsLogger.debug(LTag.PUMPCOMM, "bleSession: connected; discovering services")
        gatt.discoverServices()
        gatt.enableNotifications(notifyUuid)
        aapsLogger.debug(LTag.PUMPCOMM, "bleSession: notifications enabled")
    }

    private companion object {

        const val CONNECT_TIMEOUT_MS = 20_000L
        const val READ_TIMEOUT_MS = 15_000L

        // Safety check streams progress for ~100-210 s before the terminal frame; give it headroom.
        const val SAFETY_CHECK_TIMEOUT_MS = 250_000L

        // Three sequential basal-program writes on one connection; generous headroom over READ_TIMEOUT_MS.
        const val BASAL_PROGRAM_TIMEOUT_MS = 30_000L
        private const val RESULT_SUCCESS = 0
    }
}
