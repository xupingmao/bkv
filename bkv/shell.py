

import bkv

class Shell:
    
    def __init__(self) -> None:
        self.db = bkv.DB(db_dir="./test-data")

    def loop(self):
        while True:
            cmd = input(">>> ")
            parts = cmd.split()
            if len(parts) == 0:
                print("bad command")
                continue
            op = parts[0]
            if op == "quit" or op == "exit":
                print("Bye")
                break
            
            attr = "op_" + op
            if hasattr(self, attr):
                meth = getattr(self, attr)
                meth(parts)
            else:
                print("bad command, supported commands: get/put/set")

        self.db.close()
        
    def op_put(self, parts: list[str]):
        if len(parts) != 3:
            print("bad put command")
        else:
            key = parts[1]
            val = parts[2]
            self.db.put(key, val)
            print("OK")
    
    def op_get(self, parts):
        if len(parts) != 2:
            print("bad get command")
        else:
            key = parts[1]
            val = self.db.get(key)
            print(val)
    
    def op_compact(self, parts):
        self.db.compact()
        print("OK")

if __name__ == "__main__":
    shell = Shell()
    shell.loop()
