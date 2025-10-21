import json

if __name__ == '__main__':
    file_path = '/home/ashutosh/PycharmProjects/dagcomputeserver_v2/config/example/data/sample_trades.json'

    with open(file_path, 'r') as file:
        # Load the JSON data from the file
        trade_dict = json.load(file)

    all_topic = set()
    for trade in trade_dict:
        #'product_type': 'Derivative', 'subproduct': 'Futures'
        product_type = trade['product_type']
        subproduct = trade['subproduct']
        k_topic = f'{product_type}_{subproduct}'.replace(' ', '_').lower()
        all_topic.add(k_topic)
        print(f'{k_topic}')

    print(all_topic)