"""
This script converts our user action logs to a format usable by the
Lifeflow analysis tool.
"""
import argparse, codecs, csv, uuid
from datetime import datetime

encoding = 'iso-8859-1'

def _custom_csv_encoder(encoded_csv_data):
    for line in encoded_csv_data:
        yield line.encode(encoding)

def _custom_csv_reader(encoded_csv_data, dialect=csv.excel, **kwargs):
    csv_reader = csv.reader(_custom_csv_encoder(encoded_csv_data), dialect=dialect, **kwargs)
    for row in csv_reader:
        yield [unicode(cell, encoding) for cell in row]

# columns
# 0: itemName()
# 1: action
# 2: agent
# 3: client
# 4: component
# 5: duration
# 6: ip
# 7: map
# 8: time
# 9: user

users = { }

def convert(csv_file):
    with codecs.open(csv_file, 'rb', encoding) as f:
        csv_reader = _custom_csv_reader(f)
        for i, row in enumerate(csv_reader):
            if i == 0 and row[0] == 'itemName()': #skip
                continue
            users[row[3]] = row[9] or users.get(row[3], '')
        
    with codecs.open(csv_file, 'rb', encoding) as f:
        csv_reader = _custom_csv_reader(f)
        for i, row in enumerate(csv_reader):
            attrs = ''
            if i == 0 and row[0] == 'itemName()': #skip
                continue
            timestamp = datetime.strptime(row[8], '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y-%m-%d %H:%M:%S')
            if row[9] == '' and users[row[3]] == '':
                user = str(uuid.uuid4())
                users[row[3]] = user
            output = '%s\t%s\t%s\t%s' % (
                    users[row[3]],    # record id
                    row[1],           # event type
                    timestamp,        # date and time
                    attrs             # attrs
            )
            print output


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert a CSV of Fieldscope logs to Lifeflow format')
    parser.add_argument('csv_file', metavar='csv-file', type=str, help='a csv of Fieldscope user actions')
    args = parser.parse_args()
    convert(args.csv_file)
