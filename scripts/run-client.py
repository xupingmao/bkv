import redis
import argparse

def run_client():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, default='localhost', help='redis server host')
    parser.add_argument('--port', type=int, default=6379, help='redis server port')
    args = parser.parse_args()
    
    r = redis.Redis(host=args.host, port=args.port, db=0)
    
    server_info = f"{args.host}:{args.port}"
    
    while True:
        cmd = input(f"redis {server_info}> ")
        cmd = cmd.strip()
        if cmd == "":
            continue
        if cmd.lower() in ("exit", "quit"):
            return
        try:
            result = r.execute_command(cmd)
            if isinstance(result, bytes):
                result = result.decode("utf-8")
            print(result)
        except Exception as e:
            print(f"Error: {e}")
            
if __name__ == '__main__':
    run_client()