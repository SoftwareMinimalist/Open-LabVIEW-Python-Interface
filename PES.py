import socket
import sys
import threading
from threading import Thread


class OPIClientHandler(Thread):
    # Thread designed to handle new connections to the PES. It receives commands from LabVIEW and processes them
    # in two cases, NORT - No Return value, and EXEC - value will be returned to LV.
    # Two modes are required because there are two API methods that call into PES.

    # Constructor for new connections
    def __init__(self, connection, id):
        Thread.__init__(self)
        self.globals = {}
        self.connection = connection
        self.id = id

    # Main thread run function with cases to handle commands from LV
    def run(self):
        while True:
            # The size of the command packet is 4 bytes. it defines the behavior of the server.
            cmnd = self.connection.recv(4)

            if 'EXEC' in str(cmnd):
                print("Handler {0}: Execute command.".format(self.id))
                return_exception = "None"
                return_data = "None"

                # Receive data from LabVIEW.
                call = self.receive_data()

                try:
                    # Try to execute the code provided from LV and send back the return value.
                    # This locals business is some magic to return the data. Without this it was not working.
                    loc = locals()
                    exec("return_data" + str(self.id) + " = " + call, self.globals, loc)
                    return_data = loc['return_data' + str(self.id)]
                except Exception as e:
                    # Catch any exceptions raised and send back info to LV.
                    return_exception = e
                finally:
                    # Always send exception data, even if its empty. The protocol requires this.
                    self.send_data(return_data)
                    self.send_data(return_exception)
                    # Return acknowledgement so LV knows there was no error in transmission.
                    self.connection.sendall(b'OK')

            elif 'NORT' in str(cmnd):
                print("Handler {0}: No Return command.".format(self.id))
                return_exception = "None"

                # Receive data from LabVIEW.
                call = self.receive_data()

                try:
                    # Try to execute the code provided from LV.
                    exec(call, self.globals)
                except Exception as e:
                    # Capture only the exception info
                    return_exception = e
                finally:
                    # Send back exception info. The protocol requires OK back.
                    self.send_data(return_exception)
                    # Return acknowledgement so LV knows there was no error in transmission.
                    self.connection.sendall(b'OK')

            else:
                # Invalid command sent. The server will shout down
                print("Handler {0}: Invalid cmd or conn closed on client side. Client handler will shut down.".format(self.id))
                break

    def send_data(self, data):
        # Encode data string into bytes
        bytes_data = str(data).encode("ascii")
        # Get length of data in bytes
        data_len = len(bytes_data)
        # Convert data length to 4 bytes
        bytes_len = data_len.to_bytes(4, "big")
        # Send the size of string
        self.connection.sendall(bytes_len)
        # Send the string itself
        self.connection.sendall(bytes_data)

    def receive_data(self):
        # All data transmitted from LabVIEW will be formatted as a string with 4 byte length int in front
        b = self.connection.recv(4)
        length = int.from_bytes(b, "big", signed=True)
        # Receive the rest of the message
        b = self.connection.recv(length)
        data_string = b.decode("ascii")
        return data_string


def main():
    if len(sys.argv) == 2:
        port = int(sys.argv[1])
    else:
        port = 15000

    print("Python Execution Server started on localhost port {0}.".format(port))

    server = socket.socket()
    server.bind(('localhost', port))
    server.settimeout(0.01)  # Timeout for listening

    first_conn = False  # Used to check if there was at least one connection made before checking thread numbers
    stop = False
    id = 0

    while not stop:
        try:
            server.listen(1)
            (conn, (ip, port)) = server.accept()
        except socket.timeout:
            # In case there was a timeout on listening do nothing.
            pass
        except:
            raise
        else:
            # If there was no timeout or other exception
            print("New connection established: Handler {0}".format(id))
            # Define new thread for OPI client and start
            new_OPI_client_handler = OPIClientHandler(conn, id)
            new_OPI_client_handler.start()
            # Remember that first connection was made and increment ID
            first_conn = True
            id += 1
        finally:
            # If at least one connection was made and the number of threads == 1, all connections were
            # stopped and the main server loop can also stop.
            if first_conn:
                if len(threading.enumerate()) == 1:
                    print("All connections closed. Server will shut down.")
                    stop = True

    server.close()

main()