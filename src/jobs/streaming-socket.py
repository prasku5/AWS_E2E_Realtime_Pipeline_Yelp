import json
import socket
import time
import pandas as pd
import os


def handle_date(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    raise TypeError("Object of type '%s' is not JSON serializable" % type(obj).__name__)

# send data to the socket

# Host is the IP address of the server
# Port is the port number on the server
# Chunk size is the number of bytes to send at a time to the server
def send_data_to_socket(file_path, host="localhost", port=9999, chunk_size=2):
    '''
        docstring: 
            Send data to the socket
        params:
            file_path: str
            host: str   
            port: int
            chunk_size: int 
        return:
            None
    '''
    # create a socket object
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # AF_INET is the address family for IPv4
    # SOCK_STREAM is the socket type for TCP
    # IPv4 and TCP are the most common protocols for streaming data over the internet
    # we need both to create a socket object
      
       #IPV4  # TCP
    # ---------------       ---      :      ----
    #     Socket       +    host     :      port
    # ---------------       ---      :      ----

    # bind the socket to the host and port
    s.bind((host, port))

    s.listen(1)     # listen for incoming connections

    print('Server listening for incoming connections on the host:', host, 'and port:', port)    

    last_sent_index = 0

    while True: # loop to keep the server running

        conn, add = s.accept() # accept the connection from the client evertime
                               # previous connection gets closed automatically.

        print(f'Connected from address: {add}') # print the address of the client

        # open the file in read mode
        try:
            with open(file_path, 'r') as file:
                # skip the lines that are already read
                for _ in range(last_sent_index):
                    next(file) # for loop used to skip the lines that are already read
                
                records = [] # create a list to hold records

                for line in file:
                    records.append(json.loads(line)) # append each line to the records list
                    if len(records) == chunk_size: # check if the size of the records list equals the chunk size
                        chunk = pd.DataFrame(records) # convert the records list to a DataFrame
                        print(chunk) # print the chunk
                        for record in chunk.to_dict(orient='records'): # convert the DataFrame to a dictionary
                            serialized_data = json.dumps(record, default=handle_date).encode('utf-8') # serialize the record to JSON
                            conn.send(serialized_data + b'\n') # circuit will wait for b'\n' to know that the data is complete
                            # Then it will process the data and send the response back to the client.
                            time.sleep(5)
                            last_sent_index += 1

                        records = [] # clear the records list after sending the data in the chunk

        except (BrokenPipeError, ConnectionResetError):
            print('Connection closed by the client')
        finally:    
            conn.close()
            print('Connection closed by the server')

if __name__ == '__main__':
    # current_dir = os.path.dirname(os.path.abspath(__file__))
    # file_path = os.path.join(current_dir, '../datasets/yelp_academic_dataset_review.json')
    # send_data_to_socket(file_path=file_path)
    send_data_to_socket("datasets/yelp_academic_dataset_review.json")

# Steps

# Create socket object: The script creates a socket object using the socket module.
# Bind socket to host and port: The socket is bound to the specified host and port.
# Listen for incoming connections: The script listens for incoming connections from clients.
# Accept connection from client: When a client connects, the script accepts the connection.
# Open file in read mode: The script opens the specified file in read mode.
# Skip lines already read: If there are any lines already read, the script skips them.
# Read lines from file: The script reads lines from the file.
# Create records list: A list to hold records is created.
# Append line to records list: Each line read from the file is appended to the records list.
# Check if records list size equals chunk_size: The script checks if the size of the records list equals the specified chunk size.
# Convert records list to DataFrame: If the size matches the chunk size, the records list is converted to a DataFrame.
# Convert DataFrame to dict: The DataFrame is then converted to a dictionary.
# Serialize record to JSON: Each record in the dictionary is serialized to JSON.
# Send serialized data to client: The serialized data is sent to the client.
# Wait for 5 seconds: The script waits for 5 seconds before processing the next chunk.
# Increment last_sent_index: The script increments the last_sent_index.
# Repeat process: Steps 8-16 are repeated until all lines are read from the file.
# Handle exceptions: If an exception occurs, an error message is printed, and the connection is closed.
# Close connection and socket: Finally, the connection and socket are closed.


# +---------------------------+
# |        Start              |
# +---------------------------+
#               |
#               v
# +---------------------------+
# |   Create socket object    |
# +---------------------------+
#               |
#               v
# +---------------------------+
# | Bind socket to host/port  |
# +---------------------------+
#               |
#               v
# +---------------------------+
# | Listen for connections    |
# +---------------------------+
#               |
#               v
# +---------------------------+
# |  Accept client connection |
# +---------------------------+
#               |
#               v
# +---------------------------+
# | Open file in read mode    |
# +---------------------------+
#               |
#               v
# +---------------------------+
# | Skip lines already read   |
# +---------------------------+
#               |
#               v
# +---------------------------+
# |   Read lines from file    |
# +---------------------------+
#               |
#               v
# +---------------------------+
# |   Create records list     |
# +---------------------------+
#               |
#               v
# +---------------------------+
# | Append line to records list|
# +---------------------------+
#               |
#               v
# +---------------------------+
# | Check records list size   |
# |   (== chunk_size?)        |
# +---------------------------+
#          /       \
#         /         \
#       Yes         No
#       /             \
#      v               v
# +---------------------------+
# | Convert to DataFrame      |
# +---------------------------+
#               |
#               v
# +---------------------------+
# | Convert to dict           |
# +---------------------------+
#               |
#               v
# +---------------------------+
# | Serialize to JSON         |
# +---------------------------+
#               |
#               v
# +---------------------------+
# | Send data to client       |
# +---------------------------+
#               |
#               v
# +---------------------------+
# | Wait for 5 seconds        |
# +---------------------------+
#               |
#               v
# +---------------------------+
# | Increment last_sent_index |
# +---------------------------+
#               |
#               v
# +---------------------------+
# | Repeat process            |
# | (Read next chunk)         |
# +---------------------------+
#               |
#               v
# +---------------------------+
# |  Exception occurred?      |
# +---------------------------+
#          /       \
#         /         \
#       Yes         No
#       /             \
#      v               v
# +---------------------------+
# |  Print error message      |
# +---------------------------+
#               |
#               v
# +---------------------------+
# | Close connection          |
# +---------------------------+
#               |
#               v
# +---------------------------+
# | Close socket              |
# +---------------------------+
#               |
#               v
# +---------------------------+
# |          End              |
# +---------------------------+