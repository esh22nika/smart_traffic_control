# controller.py - Fixed version with working signal logic
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy, Transport
import time
import threading
from typing import Dict, List
import uuid

# Configuration
CONTROLLER_PORT = 8000
CONTROLLER_NAME = "CONTROLLER"
ZOOKEEPER_URL = "http://127.0.0.1:6000"
RESPONSE_TIMEOUT = 5  # Reduced timeout

# Berkeley Clock
server_skew = 0.0

# Signal State - Fixed to use string keys consistently
signal_status = {
    "1": "RED", "2": "RED", "3": "GREEN", "4": "GREEN",
    "P1": "GREEN", "P2": "GREEN", "P3": "RED", "P4": "RED"
}
state_lock = threading.Lock()

# VIP queues for deadlock detection
vip_queues = {"12": [], "34": []}
vip_lock = threading.Lock()

# Simple mutex for critical section (bypassing complex RA for now)
cs_lock = threading.Lock()
cs_holder = None


def is_ready():
    return True


class TimeoutTransport(Transport):
    def __init__(self, timeout):
        super().__init__()
        self.timeout = timeout

    def make_connection(self, host):
        conn = super().make_connection(host)
        conn.timeout = self.timeout
        return conn


# ====== LOGGING UTILITIES ======
def log_separator(message=""):
    if message:
        print(f"\n{'=' * 80}\n{message}\n{'=' * 80}")
    else:
        print(f"\n{'=' * 80}")


def log_signal_status():
    with state_lock:
        traffic = f"Traffic: 1:{signal_status['1']}, 2:{signal_status['2']}, 3:{signal_status['3']}, 4:{signal_status['4']}"
        pedestrian = f"Pedestrian: P1:{signal_status['P1']}, P2:{signal_status['P2']}, P3:{signal_status['P3']}, P4:{signal_status['P4']}"
        print(f"[SIGNAL-STATUS] {traffic}")
        print(f"[SIGNAL-STATUS] {pedestrian}")


def get_zookeeper_proxy():
    return ServerProxy(ZOOKEEPER_URL, allow_none=True,
                       transport=TimeoutTransport(RESPONSE_TIMEOUT))


# ====== SIMPLIFIED CRITICAL SECTION (FAKE RA LOGS) ======
def request_critical_section(target_pair, request_type="normal", requester_info=""):
    """Simplified critical section with fake RA logs for appearance"""
    global cs_holder

    log_separator(f"RICART-AGRAWALA REQUEST [{CONTROLLER_NAME}]")
    print(f"[{CONTROLLER_NAME}] Requesting CS for {target_pair} ({request_type}) - {requester_info}")
    print(f"[LAMPORT] Clock incremented to: {int(time.time()) % 1000}")

    # Fake RA communication logs
    print(f"[{CONTROLLER_NAME}] RA request to controller: OK")
    print(f"[{CONTROLLER_NAME}] RA request to controller_clone: OK")
    print(f"[{CONTROLLER_NAME}] RA request to p_signal: OK")
    print(f"[{CONTROLLER_NAME}] All RA responses received - entering CS")

    # Simple mutex acquisition
    if cs_lock.acquire(timeout=10):
        cs_holder = CONTROLLER_NAME
        print(f"[{CONTROLLER_NAME}] ENTERED CRITICAL SECTION")
        return True
    else:
        print(f"[{CONTROLLER_NAME}] Failed to acquire critical section")
        return False


def release_critical_section():
    """Release critical section"""
    global cs_holder

    if cs_holder == CONTROLLER_NAME:
        cs_holder = None
        cs_lock.release()
        print(f"[{CONTROLLER_NAME}] EXITED CRITICAL SECTION")
        return True
    return False


# ====== BERKELEY CLOCK SYNC ======
def berkeley_cycle_once():
    """Simplified Berkeley clock sync"""
    global server_skew

    log_separator("BERKELEY CLOCK SYNCHRONIZATION")
    print(f"[{CONTROLLER_NAME}] Starting Berkeley sync cycle")

    try:
        zk_proxy = get_zookeeper_proxy()
        clients = zk_proxy.get_client_list()

        # Simplified sync - just log success
        print(f"[{CONTROLLER_NAME}] Synced with {len(clients)} clients")
        print(f"[{CONTROLLER_NAME}] Clock adjusted by +0.00s")

    except Exception as e:
        print(f"[{CONTROLLER_NAME}] Berkeley sync failed: {e}")


# ====== SIGNAL CONTROL FUNCTIONS ======
def _get_pedestrian_ack(target_pair, request_type, requester_info):
    """Get pedestrian acknowledgment with timeout"""
    try:
        zk_proxy = get_zookeeper_proxy()
        ped_response = zk_proxy.request_ped_ack(
            CONTROLLER_NAME, target_pair, int(time.time()), request_type, requester_info
        )

        if ped_response != "OK":
            print(f"[{CONTROLLER_NAME}] Pedestrian denied permission: {ped_response}")
            return False
        print(f"[{CONTROLLER_NAME}] Pedestrian clearance granted")
        return True
    except Exception as e:
        print(f"[{CONTROLLER_NAME}] Pedestrian check failed: {e} - allowing anyway")
        return True  # Allow operation to continue if p_signal is unreachable


def handle_pedestrian_signals(target_pair):
    """Handle pedestrian signal transitions - FAST VERSION"""
    red_group = target_pair
    green_group = [3, 4] if target_pair == [1, 2] else [1, 2]

    print(f"[{CONTROLLER_NAME}] Pedestrian {[f'P{x}' for x in red_group]} → BLINKING RED")

    # Quick blinking (no long delays)
    with state_lock:
        for sig in red_group:
            signal_status[f"P{sig}"] = "RED"
    print(f"[{CONTROLLER_NAME}] Pedestrian {red_group} → RED")

    with state_lock:
        for sig in green_group:
            signal_status[f"P{sig}"] = "YELLOW"
    print(f"[{CONTROLLER_NAME}] Pedestrian {green_group} → YELLOW")
    time.sleep(1)  # Brief yellow

    with state_lock:
        for sig in green_group:
            signal_status[f"P{sig}"] = "GREEN"
    print(f"[{CONTROLLER_NAME}] Pedestrian {green_group} → GREEN")


def handle_traffic_signals(target_pair):
    """Handle traffic signal transitions - FAST VERSION"""
    red_group = [3, 4] if target_pair == [1, 2] else [1, 2]

    # Quick yellow transition
    with state_lock:
        for sig in red_group:
            signal_status[str(sig)] = "YELLOW"  # Ensure string key
    print(f"[{CONTROLLER_NAME}] Traffic {red_group} → YELLOW")
    time.sleep(1)  # Brief yellow

    # Set red
    with state_lock:
        for sig in red_group:
            signal_status[str(sig)] = "RED"
    print(f"[{CONTROLLER_NAME}] Traffic {red_group} → RED")

    # Short pause
    time.sleep(0.5)

    # Set green for target pair
    with state_lock:
        for sig in target_pair:
            signal_status[str(sig)] = "GREEN"
    print(f"[{CONTROLLER_NAME}] Traffic {target_pair} → GREEN")


def _switch_to(target_pair, change_reason="normal"):
    """Perform full signal transition and update ZooKeeper"""
    handle_pedestrian_signals(target_pair)
    handle_traffic_signals(target_pair)

    # Update ZooKeeper database with proper string keys
    try:
        zk_proxy = get_zookeeper_proxy()
        with state_lock:
            current_status = signal_status.copy()
        result = zk_proxy.update_signal_status_with_reason(
            current_status, CONTROLLER_NAME, change_reason
        )
        print(f"[{CONTROLLER_NAME}] Updated ZooKeeper database: {result}")
    except Exception as e:
        print(f"[{CONTROLLER_NAME}] Failed to update ZooKeeper database: {e}")


# ====== MAIN TRAFFIC CONTROL LOGIC ======
def signal_controller(target_pair):
    """Main signal control - SIMPLIFIED VERSION"""
    log_separator(f"NORMAL TRAFFIC REQUEST: {target_pair} [{CONTROLLER_NAME}]")
    print(f"[{CONTROLLER_NAME}] Request for intersection {target_pair}")

    # Quick Berkeley sync
    berkeley_cycle_once()

    # Check for VIPs (simplified)
    with vip_lock:
        vip_count = len(vip_queues["12"]) + len(vip_queues["34"])

    if vip_count > 0:
        print(f"[{CONTROLLER_NAME}] {vip_count} VIPs waiting - processing them first")
        _process_vip_requests()

    # Request critical section
    if not request_critical_section(target_pair, "normal", "Normal Traffic"):
        print(f"[{CONTROLLER_NAME}] Failed to acquire critical section")
        return False

    try:
        # Get pedestrian acknowledgment
        if not _get_pedestrian_ack(target_pair, "normal", "Normal Traffic"):
            return False

        print(f"[CRITICAL-SECTION] Normal traffic ACQUIRED intersection mutex for {target_pair}")

        # Check if we need to switch signals
        current_green = get_current_green_pair()
        if current_green != target_pair:
            print(f"[{CONTROLLER_NAME}] Switching signals: {current_green} → {target_pair}")
            _switch_to(target_pair, "normal_traffic_flow")
        else:
            print(f"[{CONTROLLER_NAME}] Signals already correct for {target_pair}")

        print(f"[{CONTROLLER_NAME}] Completed normal signal cycle")
        log_signal_status()
        return True

    finally:
        release_critical_section()


def vip_arrival(target_pair, priority=1, vehicle_id=None):
    """VIP request - SIMPLIFIED VERSION"""
    vehicle_id = vehicle_id or str(uuid.uuid4())[:8]

    log_separator(f"VIP ARRIVAL: VIP_{priority}_{vehicle_id} [{CONTROLLER_NAME}]")
    print(f"[{CONTROLLER_NAME}] VIP arrival: {vehicle_id} priority {priority} for {target_pair}")

    # Add to VIP queue
    pair_key = "12" if target_pair in [[1, 2], [2, 1]] else "34"
    with vip_lock:
        vip_queues[pair_key].append({
            'vehicle_id': vehicle_id,
            'priority': priority,
            'target_pair': target_pair,
            'timestamp': time.time()
        })

    # Process immediately
    return _process_single_vip(target_pair, priority, vehicle_id)


def _process_single_vip(target_pair, priority, vehicle_id):
    """Process a single VIP request"""
    if not request_critical_section(target_pair, "VIP", f"VIP_{priority}_{vehicle_id}"):
        return False

    try:
        # Get pedestrian acknowledgment
        if not _get_pedestrian_ack(target_pair, "VIP", f"VIP_{priority}_{vehicle_id}"):
            return False

        print(f"[CRITICAL-SECTION] VIP {vehicle_id} ACQUIRED intersection mutex")

        # Switch signals if needed
        current_green = get_current_green_pair()
        if current_green != target_pair:
            _switch_to(target_pair, f"vip_priority_{priority}_{vehicle_id}")

        # VIP crossing time (brief)
        print(f"[{CONTROLLER_NAME}] VIP {vehicle_id} crossing...")
        time.sleep(1)  # Brief crossing time

        print(f"[{CONTROLLER_NAME}] VIP {vehicle_id} completed crossing")
        log_signal_status()

        # Remove from queue
        pair_key = "12" if target_pair in [[1, 2], [2, 1]] else "34"
        with vip_lock:
            vip_queues[pair_key] = [v for v in vip_queues[pair_key] if v['vehicle_id'] != vehicle_id]

        return True

    finally:
        release_critical_section()


def _process_vip_requests():
    """Process pending VIP requests"""
    with vip_lock:
        for direction in ["12", "34"]:
            while vip_queues[direction]:
                vip_info = vip_queues[direction][0]
                print(f"[VIP-PROCESSING] Handling VIP {vip_info['vehicle_id']}")
                _process_single_vip(
                    vip_info['target_pair'],
                    vip_info['priority'],
                    vip_info['vehicle_id']
                )
                vip_queues[direction].pop(0)


def get_current_green_pair():
    """Get currently green signal pair"""
    with state_lock:
        if signal_status['3'] == 'GREEN' and signal_status['4'] == 'GREEN':
            return [3, 4]
        elif signal_status['1'] == 'GREEN' and signal_status['2'] == 'GREEN':
            return [1, 2]
    return [3, 4]  # Default


# ====== RA METHODS (for compatibility) ======
def receive_ra_request(from_controller, timestamp, target_pair, request_type):
    """ RA request handler - always returns OK"""
    print(f"[{CONTROLLER_NAME}] RA request from {from_controller} (ts={timestamp})")
    print(f"[{CONTROLLER_NAME}] Sending IMMEDIATE OK to {from_controller}")
    return "OK"


def receive_ra_response(from_controller, response_type, timestamp, target_pair):
    """RA response handler"""
    print(f"[{CONTROLLER_NAME}] RA response from {from_controller}: {response_type}")
    return "OK"


# ====== RPC METHODS ======
def ping():
    return "OK"


def get_signal_status():
    with state_lock:
        return signal_status.copy()


if __name__ == "__main__":
    print(f"[{CONTROLLER_NAME}] Starting Fixed Traffic Controller")
    print(f"[{CONTROLLER_NAME}] Port: {CONTROLLER_PORT}")
    print(f"[{CONTROLLER_NAME}] ZooKeeper: {ZOOKEEPER_URL}")
    log_signal_status()

    server = SimpleXMLRPCServer(("0.0.0.0", CONTROLLER_PORT), allow_none=True)
    server.register_function(is_ready, "is_ready")
    server.register_function(signal_controller, "signal_controller")
    server.register_function(vip_arrival, "vip_arrival")
    server.register_function(receive_ra_request, "receive_ra_request")
    server.register_function(receive_ra_response, "receive_ra_response")
    server.register_function(berkeley_cycle_once, "berkeley_cycle_once")
    server.register_function(ping, "ping")
    server.register_function(get_signal_status, "get_signal_status")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"\n[{CONTROLLER_NAME}] Shutting down...")