# render_deploy.py - Main deployment file for Render
import threading
import time
import subprocess
import sys
import os
import signal
import shutil
from flask import Flask, send_from_directory, jsonify

app = Flask(__name__)

# Store processes
processes = []
services_started = False


def setup_databases():
    """Copy existing database files to /tmp directory for persistence"""
    try:
        # Render has ephemeral storage, so we copy to /tmp which persists between restarts
        if os.path.exists("traffic_system.db"):
            shutil.copy2("traffic_system.db", "/tmp/traffic_system.db")
            print("✓ Copied traffic_system.db to /tmp/")

        # Copy replica databases if they exist
        for i in range(1, 4):
            if os.path.exists(f"replica_{i}.db"):
                shutil.copy2(f"replica_{i}.db", f"/tmp/replica_{i}.db")
                print(f"✓ Copied replica_{i}.db to /tmp/")
            else:
                # Create minimal replica if doesn't exist
                import sqlite3
                conn = sqlite3.connect(f"/tmp/replica_{i}.db")
                # Add basic table structure if needed
                conn.close()
                print(f"✓ Created replica_{i}.db in /tmp/")

    except Exception as e:
        print(f"Database setup error: {e}")


def modify_database_paths():
    """Modify Python files to use /tmp/ database paths"""
    files_to_modify = [
        'zookeeper.py',
        'replica_server.py',
        'master_server.py'
    ]

    for file in files_to_modify:
        if os.path.exists(file):
            try:
                with open(file, 'r') as f:
                    content = f.read()

                # Replace database paths
                content = content.replace('DB_PATH = "traffic_system.db"', 'DB_PATH = "/tmp/traffic_system.db"')
                content = content.replace("DB_PATH = 'traffic_system.db'", "DB_PATH = '/tmp/traffic_system.db'")
                content = content.replace('replica_db_path', '/tmp/' + 'replica_db_path')

                # Write back
                with open(file, 'w') as f:
                    f.write(content)
                print(f"✓ Updated database paths in {file}")

            except Exception as e:
                print(f"Could not modify {file}: {e}")


def start_service(script_name, *args):
    """Start a Python script as a subprocess"""
    try:
        cmd = [sys.executable, script_name] + list(args)
        process = subprocess.Popen(cmd)
        processes.append(process)
        print(f"✓ Started {script_name} with PID {process.pid}")
        time.sleep(3)  # Give each service time to initialize
        return process
    except Exception as e:
        print(f"✗ Error starting {script_name}: {e}")
        return None


def initialize_system():
    """Initialize all backend services"""
    global services_started

    print("🚦 Initializing Traffic Signal System...")

    # Step 1: Setup databases
    setup_databases()

    # Step 2: Modify database paths in files
    modify_database_paths()

    # Step 3: Start services in order with dependencies
    services = [
        ('replica_server.py', '7001', '1'),
        ('replica_server.py', '7002', '2'),
        ('replica_server.py', '7003', '3'),
        ('master_server.py',),
        ('zookeeper.py',),
        ('controller.py',),
        ('controlle_clone.py',),
        ('p_signal.py',),
        ('t_signal.py',),
        ('proxy_server.py',),
    ]

    for service in services:
        script = service[0]
        args = service[1:]
        print(f"🟡 Starting {script} with args {args}...")
        if start_service(script, *args):
            print(f"✅ {script} started successfully")
        else:
            print(f"❌ Failed to start {script}")

    services_started = True
    print("🎉 All backend services started!")
    print("🌐 System should be accessible now...")


@app.route('/')
def serve_frontend():
    return send_from_directory('.', 'traffic_monitor.html')


@app.route('/<path:path>')
def serve_static(path):
    return send_from_directory('.', path)


@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'services_started': services_started,
        'services_running': len(processes),
        'timestamp': time.time()
    })


@app.route('/api/test-db')
def test_db():
    """Test database connectivity"""
    try:
        import sqlite3
        conn = sqlite3.connect('/tmp/traffic_system.db')
        cursor = conn.execute('SELECT signal_id, status FROM signal_status LIMIT 5')
        signals = {row[0]: row[1] for row in cursor.fetchall()}
        conn.close()
        return jsonify({'success': True, 'signals': signals})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


def cleanup_processes():
    """Cleanup all subprocesses on exit"""
    for process in processes:
        try:
            process.terminate()
            process.wait(timeout=5)
        except:
            try:
                process.kill()
            except:
                pass


# Register cleanup handler
def signal_handler(sig, frame):
    print("🛑 Shutting down services...")
    cleanup_processes()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == '__main__':
    # Initialize system in background thread
    print("🚀 Starting Traffic Signal Monitoring System...")
    init_thread = threading.Thread(target=initialize_system, daemon=True)
    init_thread.start()

    # Start Flask app
    port = int(os.environ.get('PORT', 3000))
    print(f"📍 Web server starting on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)