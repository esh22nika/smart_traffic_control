import os

DEPLOYMENT_MODE = os.getenv('DEPLOYMENT_MODE', 'local')

def get_url(service, port):
    if DEPLOYMENT_MODE == 'production':
        return f"http://{service}:{port}"
    return f"http://127.0.0.1:{port}"

# All service URLs
ZOOKEEPER_URL = get_url('zookeeper', 6000)
MASTER_SERVER_URL = get_url('master_server', 6100)

CONTROLLER_URLS = {
    'controller': get_url('controller', 8000),
    'controller_clone': get_url('controller_clone', 8001)
}

BERKELEY_CLIENTS = {
    't_signal': get_url('t_signal', 7000),
    'p_signal': get_url('p_signal', 9000)
}

REPLICA_SERVERS = {
    'server_1': get_url('replica_server_1', 7001),
    'server_2': get_url('replica_server_2', 7002),
    'server_3': get_url('replica_server_3', 7003)
}