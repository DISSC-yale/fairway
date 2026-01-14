
import os

file_path = 'src/fairway/cli.py'
with open(file_path, 'r') as f:
    lines = f.readlines()

# Indicies are 0-based. 
# Line 94 in file is index 93.
# Line 123 in file is index 122.
# Let's double check content.

start_index = 93
end_index = 122

# Verify start
if 'config_content = f"""dataset_name: "{name}"' not in lines[start_index]:
    print(f"Start mismatch: {lines[start_index]}")
    exit(1)

# Verify end. Line 123 (index 122) should be '"""' (possibly with indentation?)
# cat -e showed '"""$' so strictly '"""\n'
if '"""' not in lines[end_index]:
    print(f"End mismatch: {lines[end_index]}")
    # Search for the end...
    for i in range(start_index, start_index + 40):
        if lines[i].strip() == '"""':
            end_index = i
            break
    print(f"Found end at {end_index}")

# Construct new content
new_content = [
    "    engine_val = 'pyspark' if engine == 'spark' else 'duckdb'\n",
    "    config_content = FAIRWAY_YAML_TEMPLATE.format(name=name, engine=engine_val)\n"
]

# Replace
lines[start_index:end_index+1] = new_content

with open(file_path, 'w') as f:
    f.writelines(lines)

print("Successfully updated src/fairway/cli.py")
