import json

if __name__ == '__main__':
    file_path = '/home/ashutosh/PycharmProjects/dagcomputeserver_v2/config/example/data/sample_trades.json'

    with open(file_path, 'r') as file:
        # Load the JSON data from the file
        trade_dict = json.load(file)
    for trade in trade_dict:
        print(trade_dict)