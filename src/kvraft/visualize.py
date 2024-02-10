# import re
# from rich.console import Console

# def parse_log_line(line):
#     pattern = re.compile(r'\[(\d+)\] (\w+) client \[(\d+)\] requestid \[(\d+)\]')
#     match = pattern.search(line)
    
#     if match:
#         server_id, action, client_id, request_id = map(int, match.groups())
#         return {
#             'server_id': server_id,
#             'action': action,
#             'client_id': client_id,
#             'request_id': request_id
#         }
#     else:
#         return None

# def print_colored_data(parsed_data):
#     console = Console()

#     if parsed_data:
#         client_id = parsed_data['client_id']
#         color = f'bold blue'  # You can choose different colors based on client_id
#         console.print(f"[{color}]Server ID: {parsed_data['server_id']}[/] "
#                       f"[{color}]Action: {parsed_data['action']}[/] "
#                       f"[{color}]Client ID: {client_id}[/] "
#                       f"[{color}]Request ID: {parsed_data['request_id']}[/]")
#     else:
#         console.print("No match found.")



# def read_file():
#     file_path = 'demo.txt'  # Replace 'your_file.txt' with your file path
#     with open(file_path, 'r') as file:
#         # Read and print each line
#         for line in file:
#             parsed_data = parse_log_line(line)
#             print_colored_data(parsed_data)

#!/usr/bin/env python3
import sys
import re
from rich.console import Console

def set_string_size(input_string, size):
    if len(input_string) > size:
        # Truncate if the string is longer than the specified size
        return input_string[:size]
    else:
        # Pad if the string is shorter than the specified size
        return input_string.ljust(size)

def parse_log_line(line):
    pattern = re.compile(r'\[(\d+)\] (\w+) client \[(\d+)\] requestid \[(\d+)\]')
#  index \[(\d+)\]
    match = pattern.search(line)
    
    if match:
        server_id, action, client_id, request_id = map(str, match.groups())
        # return {
        #     'server_id': server_id,
        #     'action': set_string_size(action, 10),
        #     'client_id': client_id,
        #     'request_id': request_id
        # }
        return server_id, set_string_size(action, 10), client_id, request_id
    else:
        return None


colors = [
        "bright_cyan",
        "yellow",
        "green",
        "blue",
        "cyan",
        "magenta",
        "white",
        "bright_blue",
        "bright_red",
        "bright_green",
        "bright_magenta",
        "bright_white",
        "bright_yellow",
        "red"
    ]


color_map = {}


def get_colors():
    global colors
    new_color = colors[0]
    colors = colors[1:] + [new_color]

    return new_color



def print_colored_log(server_id, action, client_id, request_id):
    console = Console()
    
    if client_id not in color_map:
        # print(color_map)
        color_map[client_id] = get_colors()

    log_message = f"[{color_map[client_id]}] SERVER {server_id} {action} CLIENT {client_id} REQ_ID {request_id}[/{color_map[client_id]}]"

    console.print(log_message)

def main():
    file_path = 'demo.txt'  # Replace 'your_file.txt' with your file path
    cnt = 0
    with open(file_path, 'r') as file:
        # Read and print each line
        for line in file:
            parsed = parse_log_line(line.strip())
            
            if parsed:
                server_id, action, client_id, request_id = parsed
                # if action != set_string_size("RESPONSE", 10):
                if cnt > 3000:
                    print_colored_log(server_id, action, client_id, request_id)
                cnt += 1
                if cnt % 1000 == 0:
                    user_input = input("Press Enter to continue...")

if __name__ == "__main__":
    main()
