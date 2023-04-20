import json
from pathlib import Path
from ast import literal_eval
from json.decoder import JSONDecodeError

folder = Path.cwd().parent / 'dataset'
folder.mkdir(exist_ok=True)
files = folder.glob("*.json")

business = str(next(files))
checkin = str(next(files))
review = str(next(files))
tip = str(next(files))
user = str(next(files))

out_folder = Path.cwd().parent / 'dataset' / 'Clean Data'
out_folder.mkdir(exist_ok=True)
out_file_business = str(out_folder / 'yelp_output_business.json')
out_file_checkin = str(out_folder / 'yelp_output_checkin.json')
out_file_user = str(out_folder / 'yelp_output_user.json')


def fix_json_formatting(input_file: str, output_file: str):
    with open(input_file, 'r', encoding='utf-8') as f:
        # Read the input file as a string
        data_str = f.read()

        # Split the string into lines, if multiple objects are present
        data_lines = data_str.strip().split('\n')

        # Parse each line as a separate JSON object
        for line in data_lines:
            try:
                data = json.loads(line)
            except JSONDecodeError:
                print(f"Error decoding JSON object in line: {line}")
                continue

            attributes = data.get('attributes')
            if attributes:
                for key, value in attributes.items():
                    if isinstance(value, str) and value.startswith("u'") and value.endswith("'"):
                        data['attributes'][key] = value[2:-1]
                    # To evaluate and store the value as a JSON object, we use literal_eval
                    elif isinstance(value, str) and value.startswith('{') and value.endswith('}'):
                        data['attributes'][key] = literal_eval(value)
                    elif isinstance(value, str) and value.startswith("'") and value.endswith("'"):
                        data['attributes'][key] = value[1:-1]

            categories = data.get('categories')
            if categories:
                data['categories'] = data['categories'].replace(r"\/", ", ")
                data['categories'] = categories.split(', ')

            # Write the cleaned JSON object to the output file
            with open(output_file, 'a') as c:
                json.dump(data, c)
                c.write('\n')


def transform_list_len(input_file: str, output_file: str, item: str):
    with open(input_file, 'r', encoding='utf-8') as r:
        data_str = r.read()
        data_lines = data_str.strip().split('\n')

        for line in data_lines:
            try:
                data = json.loads(line)
            except JSONDecodeError:
                print(f"Error decoding JSON object in line: {line}")
                continue

            dates_list = data[item].split(", ")
            dates_len = len(dates_list)
            data[item] = dates_len

            with open(output_file, 'a') as w:
                json.dump(data, w)
                w.write('\n')


fix_json_formatting(business, out_file_business)
print('Transformation of Business well done!')
transform_list_len(checkin, out_file_checkin, "date")
print('Transformation of Checkin well done!')
transform_list_len(user, out_file_user, "friends")
print('Transformation of user well done!')

