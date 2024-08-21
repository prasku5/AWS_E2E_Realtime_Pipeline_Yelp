import json
import socket
import time
import pandas as pd
import os


def handle_date(obj): # function to handle date serialization
    if isinstance(obj, pd.Timestamp): # check if the object is a pandas Timestamp
        return obj.strftime('%Y-%m-%d %H:%M:%S') # return the formatted date. Here we are formatting the date to 'YYYY-MM-DD HH:MM:SS'
    raise TypeError("Object of type '%s' is not JSON serializable" % type(obj).__name__) # raise a TypeError if the object is not a pandas Timestamp

# send data to the socket

# Host is the IP address of the server where the socket is running
# Port is the port number on the server where the socket is listening 
# Chunk size is the number of bytes to send at a time to the server from the file 
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
    
    # AF_INET is the address family for IPv4 - This will allow us to use an "IP address" to connect to the server, when we mean server here we are referring to the socket server.

    # SOCK_STREAM is the socket type for TCP - This will allow us to stream data over the internet. it will ensure that the data is delivered in the order it was sent and that it is error-free.
    
    # so we are creating a socket object that will stream data over the internet using an IP address and TCP protocol.
    
    # we need both to create a socket object because we are going to stream data over the internet using an IP address and TCP   
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
    # so we created a socket object and bind it to the host and port and it will listen for incoming connections using a server   

    last_sent_index = 0

    while True: # loop to keep the server running

        conn, add = s.accept() # accept the connection from the client evertime
                               # previous connection gets closed automatically.

        print(f'Connected from address: {add}') # print the address of the client

        # open the file in read mode
        try:
            with open(file_path, 'r') as file:
                # skip the lines that are already read
                for _ in range(last_sent_index): # It will be able to skip since we are using the _ variable to iterate over the range
                    # _ is a throwaway variable in Python. It is used when you don't need the variable in the loop.
                    next(file) # for loop used to skip the lines that are already read
                
                # File Content After Skipping:
                # +---------------------------------------------------------------+
                # | [Skipped] Line 1                                              |
                # | [Skipped] Line 2                                              |
                # | [Skipped] Line 3                                              |
                # | Line 4  <--- File Pointer Starts Here                         |
                # | Line 5                                                       |
                # | Line 6                                                       |
                # | ...                                                           |
                # | Line N                                                       |
                # +---------------------------------------------------------------+

                records = [] # create a list to hold records

                for line in file:
                    records.append(json.loads(line)) # append each line to the records list - converting json to python dictionary
                    if len(records) == chunk_size: # check if the size of the records list equals the chunk size
                        chunk = pd.DataFrame(records) # convert the records list to a DataFrame

                        # we are converting to a DataFrame because it is easier to work with data in a tabular format
                        
                        # For example chunk looks like this:
                        # +---------------------------------------------------------------+
                        # |  Column 1  |  Column 2  |  Column 3  |  ...  |  Column N  |
                        # +---------------------------------------------------------------+
                        # |  Value 1   |  Value 2   |  Value 3   |  ...  |  Value N   |
                        # |  Value 1   |  Value 2   |  Value 3   |  ...  |  Value N   |
                        # |  Value 1   |  Value 2   |  Value 3   |  ...  |  Value N   |
                        # |  ...       |  ...       |  ...       |  ...  |  ...       |

                        print(chunk) # print the chunk
                        for record in chunk.to_dict(orient='records'): # convert the DataFrame to a dictionary
                            
                            serialized_data = json.dumps(record, default=handle_date).encode('utf-8') # serialize the record to JSON
                            # encode utf-8 is used to convert the string to bytes. utf-8 is the most common encoding used for text data.
                            # handle the date serialization using the handle_date function
                            # default parameter is used to specify a function to handle the serialization of non-standard objects
                            # in our datagframe we have a pandas Timestamp object which is a non-standard object for JSON serialization.
                              
                            conn.send(serialized_data + b'\n') # circuit will wait for b'\n' to know that the data is complete
                            # Then it will process the data and send the response back to the client.
                            time.sleep(5) # wait for 5 seconds before sending the next chunk of data to the client 
                            last_sent_index += 1 # increment the last_sent_index by 1 since we have sent the data in the chunk

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
# |   Create socket object    | --> socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
# +---------------------------+
#               |
#               v
# +---------------------------+
# | Bind socket to host/port  | --> socket.bind((host, port)) 
# +---------------------------+
#               |
#               v
# +---------------------------+
# | Listen for connections    | ---> socket.listen(1) 
# +---------------------------+
#               |
#               v
# +---------------------------+
# |  Accept client connection | --> socket.accept()
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
# | Convert to dict           | --> DataFrame.to_dict(orient='records')
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