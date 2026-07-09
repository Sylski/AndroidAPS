package app.aaps.pump.carelevo.ble

import app.aaps.core.interfaces.pump.ble.BleTransport

/**
 * CareLevo-specific extension of the shared fleet [BleTransport].
 *
 * Adopting [BleTransport] (the same abstraction Dana/Equil/Medtrum run on) is a code-unification
 * step: the coroutine correlation layer stays in [BleClient]; only the transport underneath is
 * swapped from the bespoke `AndroidGattConnection` to this shared contract. See
 * `_docs/carelevo-new-ble-stack.md` for the migration plan.
 *
 * Adds the two fields the fleet impls carry that aren't part of the generic interface:
 * - [scanAddress]: MAC filter for [app.aaps.core.interfaces.pump.ble.BleScanner.startScan]
 * - [onGattError133]: hook for the status-133 bond-lock workaround
 *
 * Implemented by [CarelevoBleTransportImpl] (production). A future
 * `CarelevoEmulatorBleTransport` in a `:pump:carelevo-emulator` module can implement the same
 * interface for hardware-free testing, swapped in via DI exactly like Dana/Equil.
 */
interface CarelevoBleTransport : BleTransport {

    /** MAC address filter for scanning. Set before calling `scanner.startScan()`. */
    var scanAddress: String?

    /** Called when GATT error 133 occurs, before `onConnectionStateChanged(false)`. */
    var onGattError133: (() -> Unit)?
}
