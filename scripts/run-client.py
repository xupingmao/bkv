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
            print("Bye!")
            return
        try:
            result = r.execute_command(cmd)
            if isinstance(result, bytes):
                result = result.decode("utf-8")
            elif isinstance(result, list):
                print_list_resp(result)
                continue
            print(result)
        except Exception as e:
            print(f"Error: {e}")

def print_list_resp(result:list):
    for i, item in enumerate(result):
        if isinstance(item, bytes):
            item = item.decode("utf-8")
        print(f"{i+1}) {item}")

if __name__ == '__main__':
    run_client()