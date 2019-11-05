import asyncio


class ServerError(Exception):
    pass


class AnaliseError(ServerError):
    pass


class ConnectionError(ServerError):
    pass


class Memory:
    # Class for storage of metrics.
    def __init__(self):
        self.store = {}

    # Finding information about metrics.
    def find(self, key):
        item = self.store

        if key != "*":
            if key in item:
                it = item.get(key)
                item = dict()
                item[key] = it
            else:
                item = dict()
                item[key] = {}

        metric = dict()
        for key, timestamp in item.items():
            metric[key] = sorted(timestamp.items())

        return metric

    # Building te main dictionary with metrics.
    def build(self, key, value, timestamp):
        if key not in self.store:
            self.store[key] = dict()

        self.store[key][timestamp] = value


class ClientServerProtocol(asyncio.Protocol):
    memory = Memory()

    def __init__(self):
        super().__init__()
        self.datas = b''

    # Different argument for put and get fo None.
    def main(self, method, name, value=None, timestamp=None):
        if method == "put":
            return self.memory.build(name, value, timestamp)
        elif method == "get":
            return self.memory.find(name)
        else:
            raise ValueError

    # Working with data and messages.
    def process_data(self, data):
        parts_of_message = data.split("\n")
        commands = []
        for part in parts_of_message:
            if not part:
                continue

            try:
                method, params = part.strip().split(" ", 1)
                if method == "put":
                    key, value, timestamp = params.split()
                    commands.append((method, key, float(value), int(timestamp)))
                elif method == "get":
                    commands.append((method, params))
                else:
                    self.transport.write(f"error\nwrongcommand\n\n".encode())
            except ServerError:
                pass

        responses = []
        for command in commands:
            resp = self.main(*command)
            responses.append(resp)

        row = []

        for response in responses:
            if not response:
                continue
            for key, values in response.items():
                for timestamp, value in values:
                    row.append(f"{key} {value} {timestamp}")

        result = "ok\n"

        if row:
            result += "\n".join(row) + "\n"

        return result + "\n"

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        self.datas += data
        try:
            decoded_data = self.datas.decode()
        except UnicodeDecodeError:
            return

        if not decoded_data.endswith('\n'):
            return

        self.datas = b''

        try:
            resp = self.process_data(decoded_data)
        except (AnaliseError, ServerError) as err:
            # формируем ошибку, в случае ожидаемых исключений
            self.transport.write(f"error\n{err}\n\n".encode())
            return

        self.transport.write(resp.encode())


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
