from .corintick import Corintick

if __name__ == '__main__':
    api = Corintick()
    read = api.read
    write = api.write
