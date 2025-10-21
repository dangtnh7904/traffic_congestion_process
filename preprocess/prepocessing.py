import json

input_file = 'data/hanoi_traffic_data.json'
output_file = 'data/hanoi_traffic_data.jsonl' # line json file
time_stamp = 'data/timestamp.jsonl'

with open(input_file, 'r') as infile, open(output_file, 'w') as outfile, open(time_stamp, 'w') as output_time:

    full_content = json.load(infile)
    result_list = full_content.get('results', [])
    time_stamp = full_content.get('sourceUpdated', []) # Assuming timestamp is a list with one item

    if time_stamp:
        output_time.write(f'{{"time_stamp": "{time_stamp}"}}\n')

    for item in result_list:
        json_line = json.dumps(item)
        outfile.write(json_line + '\n')

print(f"Converted {input_file} to {output_file} in JSON Lines format.")