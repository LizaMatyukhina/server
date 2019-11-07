import asyncio


class ServerError(Exception):
    pass


class Memory:
    # Class for storage of metrics.
    def __init__(self):
        self.store = {}

    # Finding information about metrics.
    def find(self, key):
        resp = 'ok\n'
        if key == '*':
            for key, value in self.store.items():
                for v in value:
                    resp += f'{key} {v[1]} {v[0]}\n'

        elif key in self.store:
            for v in self.store[key]:
                resp += f'{key} {v[1]} {v[0]}\n'
        return resp + '\n'

    # Building te main dictionary with metrics.
    def build(self, key, value, timestamp):
        if key == '*':
            return 'error\nwrong command\n\n'
        if key not in self.store:
            self.store[key] = []
        if (timestamp, value) not in self.store[key]:
            self.store[key].append((timestamp, value))
            self.store[key].sort(key=lambda x: x[0])
        return 'ok\n\n'


class ClientServerProtocol(asyncio.Protocol):
    memory = Memory()

    def __init__(self):
        pass

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        try:
            resp = self.process_data(data.decode("utf-8").strip('\n'))
            self.transport.write(resp.encode("utf-8"))
        except ServerError:
            pass

    def process_data(self, data):
        try:
            main = data.split(' ')
            if main[0] == 'get':
                return self.memory.find(main[1])
            elif main[0] == 'put':
                return self.memory.build(main[1], main[2], main[3])
            else:
                return 'error\nwrong command\n\n'
        except ServerError:
            pass


def run_server(host, port):
    try:
        loop = asyncio.get_event_loop()
        coro = loop.create_server(ClientServerProtocol, host, port)
        server = loop.run_until_complete(coro)

        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass

        server.close()
        loop.run_until_complete(server.wait_closed())
        loop.close()
    except ConnectionError:
        pass


if __name__ == "__main__":
    run_server("127.0.0.1", 8888)
