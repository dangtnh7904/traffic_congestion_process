import json 
input_file = 'data\hanoi_traffic_data.jsonl'

# take the first 5 lines from the input file and print them
with open(input_file, 'r') as infile, open('data\sample_output.jsonl', 'w') as outfile:
    for _ in range(1):
        line = infile.readline()
        pretty_line = json.dumps(json.loads(line), indent=2)
        outfile.write(pretty_line + "\n")
