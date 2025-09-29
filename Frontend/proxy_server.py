# proxy_server.py - Extended to handle POST requests for traffic/vip
from http.server import HTTPServer, BaseHTTPRequestHandler
from xmlrpc.client import ServerProxy, Transport
import json
import time
from urllib.parse import urlparse

# Configuration
PROXY_PORT = 8080
CONTROLLER_URLS = {
    'controller': 'http://127.0.0.1:8000',
    'controller_clone': 'http://127.0.0.1:8001'
}


class TimeoutTransport(Transport):
    def __init__(self, timeout):
        super().__init__()
        self.timeout = timeout

    def make_connection(self, host):
        conn = super().make_connection(host)
        conn.timeout = self.timeout
        return conn


class ProxyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            parsed_url = urlparse(self.path)

            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
            self.send_header('Access-Control-Allow-Headers', 'Content-Type')
            self.end_headers()

            if parsed_url.path == '/api/signals':
                response_data = self.fetch_all_controller_data()
                self.wfile.write(json.dumps(response_data).encode())
            else:
                self.wfile.write(json.dumps({'error': 'Unknown endpoint'}).encode())

        except Exception as e:
            self.send_error(500, str(e))

    def do_POST(self):
        """Handle POST requests from web frontend (manual requests)"""
        try:
            parsed_url = urlparse(self.path)
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length).decode('utf-8')
            data = json.loads(body)

            req_type = data.get('method') or data.get('type')
            params = data.get('params') or []

            # Default response
            result = {"status": "error", "message": "Invalid request"}

            # Pick primary controller (first one in dict)
            controller_url = list(CONTROLLER_URLS.values())[0]
            proxy = ServerProxy(controller_url, allow_none=True, transport=TimeoutTransport(5))

            if parsed_url.path == '/api/request':
                if req_type == 'signal_request':
                    target_pair = params[1] if len(params) > 1 else [1, 2]
                    print(f"[PROXY] Traffic request for signals {target_pair}")
                    ok = proxy.signal_controller(target_pair)
                    result = {"status": "ok" if ok else "fail", "action": "signal_request", "target_pair": target_pair}

                elif req_type == 'vip_request':
                    target_pair = params[1] if len(params) > 1 else [1, 2]
                    print(f"[PROXY] VIP request for signals {target_pair}")
                    ok = proxy.vip_arrival(target_pair)
                    result = {"status": "ok" if ok else "fail", "action": "vip_request", "target_pair": target_pair}

                elif req_type == 'deadlock':
                    print(f"[PROXY] Deadlock simulation requested")
                    # Create two competing VIP requests
                    try:
                        proxy.vip_arrival([1, 2], 1, "DEADLOCK_VIP_A")
                        proxy.vip_arrival([3, 4], 1, "DEADLOCK_VIP_B")
                        result = {"status": "ok", "action": "deadlock", "message": "Deadlock scenario created"}
                    except Exception as e:
                        result = {"status": "fail", "action": "deadlock", "message": str(e)}

                else:
                    result = {"status": "error", "message": f"Unknown method: {req_type}"}

            else:
                # Handle the old POST format for backward compatibility
                if req_type == 'signal_request':
                    target_pair = params[1] if len(params) > 1 else [1, 2]
                    ok = proxy.signal_controller(target_pair)
                    result = {"status": "ok" if ok else "fail", "action": "signal_request", "target_pair": target_pair}

                elif req_type == 'vip_request':
                    target_pair = params[1] if len(params) > 1 else [1, 2]
                    ok = proxy.vip_arrival(target_pair)
                    result = {"status": "ok" if ok else "fail", "action": "vip_request", "target_pair": target_pair}

                elif req_type == 'deadlock':
                    result = {"status": "ok", "action": "deadlock", "message": "Deadlock simulation triggered"}

            # Send response
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(result).encode())

        except Exception as e:
            print(f"[PROXY] Error handling POST request: {e}")
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({'error': str(e)}).encode())

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def fetch_controller_data(self, controller_name):
        try:
            url = CONTROLLER_URLS[controller_name]
            transport = TimeoutTransport(5)
            proxy = ServerProxy(url, allow_none=True, transport=transport)

            signals = proxy.get_signal_status()
            try:
                proxy.ping()
                online = True
            except:
                online = False

            return {
                'controller': controller_name,
                'online': online,
                'signals': signals,
                'timestamp': time.time(),
                'last_update': time.strftime('%Y-%m-%d %H:%M:%S')
            }
        except Exception as e:
            return {
                'controller': controller_name,
                'online': False,
                'signals': {},
                'error': str(e),
                'timestamp': time.time(),
                'last_update': time.strftime('%Y-%m-%d %H:%M:%S')
            }

    def fetch_all_controller_data(self):
        response = {}
        for controller_name in CONTROLLER_URLS.keys():
            response[controller_name] = self.fetch_controller_data(controller_name)
        response['proxy_timestamp'] = time.time()
        return response

    def log_message(self, format, *args):
        # Only show our custom messages, suppress default HTTP logs
        pass


if __name__ == "__main__":
    print("=" * 60)
    print("TRAFFIC SIGNAL PROXY SERVER (Enhanced)")
    print("=" * 60)
    print(f"Frontend API: http://localhost:{PROXY_PORT}/api/signals")
    print(f"Request API: http://localhost:{PROXY_PORT}/api/request")
    print("Available actions: signal_request, vip_request, deadlock")
    print("=" * 60)

    server = HTTPServer(('localhost', PROXY_PORT), ProxyHandler)
    try:
        print(f"Proxy server ready on http://localhost:{PROXY_PORT}")
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down proxy server...")
        server.shutdown()