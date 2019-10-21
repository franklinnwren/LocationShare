import asyncio, aiohttp, sys, json, time


API_KEY = "AIzaSyAUni5-FyjqtWJpjts5tDDDuTU5AqLXPLE"
port_assignment = {
    'Goloman': 12529,
    'Hands': 12530,
    'Holiday': 12535,
    'Welsh': 12532,
    'Wilkes': 12534,
}
server_connection = {
    'Goloman': ['Hands', 'Holiday', 'Wilkes'],
    'Hands': ['Goloman', 'Wilkes'],
    'Holiday': ['Goloman', 'Welsh', 'Wilkes'],
    'Welsh': ['Holiday'],
    'Wilkes': ['Goloman', 'Hands', 'Holiday'],
}
client_message = {}


async def task_assignment(processed_input):
    if (processed_input == None or len(processed_input) == 0):
        return 0
    elif (processed_input[0] != "IAMAT" and processed_input[0] != "WHATSAT" and processed_input[0] != "AT"):
        return 0
    elif (processed_input[0] == "IAMAT"):
        if(len(processed_input) != 4):
            return 0
        else:
            return 1
    elif (processed_input[0] == "WHATSAT"):
        if(len(processed_input) != 4):
            return 0
        else:
            return 2
    elif (processed_input[0] == "AT"):
        if(len(processed_input) != 6):
            return 0
        else:
            return 3
    else:
        return 0


async def iamat(processed_input):
    position_tuple = None
    time = None
    splitter = None
    if (processed_input[2][0] != '+' and processed_input[2][0] != '-'):
        return (None, None, None, None, None)
    if (processed_input[2][-1] == '+' or processed_input[2][-1] == '-'):
        return (None, None, None, None, None)
    for iter in range(1, len(processed_input[2])):
        if (processed_input[2][iter] == '+' or processed_input[2][iter] == '-'):
            splitter = iter
    if (splitter == None):
        return (None, None, None, None, None)
    try:
        position_tuple = (processed_input[2][0:splitter], processed_input[2][splitter:])
        float(position_tuple[0])
        float(position_tuple[1])
    except:
        return (None, None, None, None, None)
    try:
        time = float(processed_input[3])
    except:
        return (None, None, None, None, None)
    return (processed_input[1], processed_input[2], processed_input[3], position_tuple, time)


async def whatsat(processed_input):
    radius = None
    bound = None
    try:
        radius = int(processed_input[2])
    except:
        return (None, None, None, None, None)
    if (radius > 50 or radius <= 0):
        return (None, None, None, None, None)
    try:
        bound = int(processed_input[3])
    except:
        return (None, None, None, None, None)
    if (bound > 20 or bound <= 0):
        return (None, None, None, None, None)
    return (processed_input[1], processed_input[2], processed_input[3], str(radius * 1000), bound)
        

async def at(processed_input):
    time_difference = None
    if (processed_input[1] not in port_assignment):
        return (None, None, None, None, None)
    if (processed_input[2][0] != '+' and processed_input[2][0] != '-'):
        return (None, None, None, None, None)
    try:
        time_difference = float(processed_input[2])
    except:
        return (None, None, None, None, None)
    pre_temp = ["IAMAT", processed_input[3], processed_input[4], processed_input[5]]
    temp = await iamat(pre_temp)
    if (temp == (None, None, None, None, None)):
        return (None, None, None, None, None)
    else:
        return (processed_input[1], time_difference, temp[0], temp[1], temp[2])

        
async def flood(message, server):
    for iter in server_connection[server]:
        log.write("Connecting to " + iter + "\n")
        try:
            reader, writer = await asyncio.open_connection('127.0.0.1', port_assignment[iter], loop=loop)
            log.write("Connected to " + iter + "\n")
            writer.write(message.encode())
            await writer.drain()
            log.write(server + " Disconnected with " + iter + "\n")
            log.write("Writer is closed" + "\n")
            writer.close()
        except:
            log.write("Error in connection with " + iter + "\n")


async def get_output(category, useful_tuple, time):
    output = ""
    if (category == 1):
        time_difference = (float(time) - useful_tuple[4])
        if (time_difference > 0):
            time_out = "+" + str(time_difference)
        else:
            time_out = str(time_difference)
        output = "AT " + sys.argv[1] + " " + time_out + " " + useful_tuple[0] + " " + useful_tuple[1] + " " + useful_tuple[2] + "\n"
        client_name = useful_tuple[0]
        client_message[client_name] = (useful_tuple, time_out, server_name, time)
        communication = "AT " + sys.argv[1] + " " + time_out + " " + useful_tuple[0] + " " + useful_tuple[1] + " " + useful_tuple[2] + "\n"
        await flood(communication, sys.argv[1])
    elif (category == 2):
        if (useful_tuple[0] in client_message):
            assigned_client = client_message[useful_tuple[0]]
            coordinates = assigned_client[0][3]
            latitude = coordinates[0].replace("+","")
            longitude = coordinates[1].replace("+","")
            input_to_google = latitude + "," + longitude
            radius = useful_tuple[3]
            output_first = "AT " + assigned_client[2] + " " + assigned_client[1] + " " + assigned_client[0][0] + " " + assigned_client[0][1] + " " + assigned_client[0][2] + "\n"
            results = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?location=" + input_to_google + "&radius=" + radius + "&key=" + API_KEY
            async with aiohttp.ClientSession() as session:
                async with session.get(results, ssl=False) as response:
                    get_json = await response.json()
                    bounded_results = get_json['results'][0:useful_tuple[4]]
                    get_json['results'] = bounded_results
                    output = output_first + json.dumps(get_json, indent = 3) + "\n\n"
        else:
            output = "? " + "WHATSAT " + useful_tuple[0] + " " + useful_tuple[1] + " " + useful_tuple[2] + "\n" 
            log.write("Error in message: " + output + "\n")
            log.write("Writer is closed" + "\n")
    elif (category == 3 and (useful_tuple[2] not in client_message or float(useful_tuple[4]) > client_message[useful_tuple[2]][3])):
        post_processing = await iamat(["IAMAT", useful_tuple[2], useful_tuple[3], useful_tuple[4]])
        time_out = ""
        if (useful_tuple[1] < 0):
            time_out = str(useful_tuple[1])
        else:
            time_out = '+' + str(useful_tuple[1])
        client_message[useful_tuple[2]] = ((useful_tuple[2], useful_tuple[3], useful_tuple[4], post_processing[3], post_processing[4]), time_out, useful_tuple[0], time)
        communication = "AT " + useful_tuple[0] + " " + time_out + " " + useful_tuple[2] + " " + useful_tuple[3] + " " + useful_tuple[4] + "\n"
        await flood(communication, sys.argv[1])
    else:
        pass
    return output
    

async def back_end(reader, writer):
    input_line = await reader.readline()
    input_time = time.time()
    log.write("Message: " + input_line.decode())
    processed_input = input_line.decode().strip().split()
    category = await task_assignment(processed_input)
    useful_tuple_1 = None
    useful_tuple_2 = None
    useful_tuple_3 = None
    output = None
    if (category == 1):
        useful_tuple_1 = await iamat(processed_input)
    elif (category == 2):
        useful_tuple_2 = await whatsat(processed_input)
    elif (category == 3):
        useful_tuple_3 = await at(processed_input)
    else:
        output = ""
    if (category == 1 and useful_tuple_1 != (None, None, None, None, None)):
        output = await get_output(1, useful_tuple_1, input_time)
    elif (category == 2 and useful_tuple_2 != (None, None, None, None, None)):
        output = await get_output(2, useful_tuple_2, input_time)
    elif (category == 3 and useful_tuple_3 != (None, None, None, None, None)):
        output = await get_output(3, useful_tuple_3, input_time)
    else:
        output = ""
    if (output != ""):
        log.write("Receiving " + input_line.decode() + "\n")
        log.write("Sending " + output + "\n")
        writer.write(output.encode())
        await writer.drain()
        log.write("Writer is closed" + "\n")
        writer.close()
    else:
        output = "? " + input_line.decode()
        log.write("Error in message: " + output + "\n")
        writer.write(output.encode())
        await writer.drain()
        log.write("Writer is closed" + "\n")
        writer.close()


def main():
    global log
    global loop
    global server_name
    if len(sys.argv) != 2:
        print("Fatal error: must input exactly one server name")
        sys.exit(1)
    elif sys.argv[1] not in port_assignment:
        print("Fatal error: server name not recognizable")
        sys.exit(1)
    else:
        server_name = sys.argv[1]
        log = open(server_name, "w+")
        loop = asyncio.get_event_loop()
        coro = asyncio.start_server(back_end, '127.0.0.1', port_assignment[server_name], loop=loop)
        server = loop.run_until_complete(coro)
        server_message = "Server {0} starts at port {1}".format(server_name, port_assignment[server_name])
        log.write(server_message + "\n")
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass
        server_message = "Server {0} at port {1} is closing".format(server_name, port_assignment[server_name])
        log.write(server_message + "\n")
        server.close()
        loop.run_until_complete(server.wait_closed())
        loop.close()
        log.close()

if __name__ == "__main__":
    main()
