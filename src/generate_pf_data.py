import boto3
import pandas as pd
import json
import random
from random import seed

seed(5)
name_list = ['Oscar', 'Max', 'Tiger', 'Sam', 'Misty',
            'Simba', 'Coco', 'Chloe', 'Lucy', 'Missy',
            'Molly', 'Tigger', 'Smokey', 'Milo', 'Cleo',
            'Sooty', 'Monty', 'Puss', 'Kitty','Felix',
            'Bella', 'Jack', 'Lucky', 'Casper', 'Charlie',
            'Thomas', 'Toby', 'Ginger', 'Oliver', 'Daisy',
            'Gizmo', 'Muffin', 'Jessie', 'Sophie', 'Fluffy',
            'Sebastian', 'Billy', 'Jasper', 'Jasmine', 'Sasha',
            'Zoe', 'Phoebe', 'Tom', 'Lilly', 'Sylvester',
            'George', 'Kimba', 'Harry', 'Holly', 'Minnie',
            'Tsuke']
state_pd = pd.read_csv('/Users/alexandraplassaras/src/furever_match/data/input/abbr-list.csv')


def generate(key, num):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket='fureverdump', Key=key)
    text = obj["Body"].read().decode()
    json_text = json.loads(text)

    new_df = json_text['animals']

    new_list = []
    for row in new_df:
        row['id'] = random.randint(0, 10000000)
        row['name'] = random.choice(name_list)
        row['contact']['address']['state'] = str(state_pd.iloc[random.randint(0, 58)])[8:10]
        new_list.append(row)
    expected_output = {'animals': new_list}

    filename = 'expected_output_' + str(num) + '.json'
    s3d = boto3.resource('s3')
    s3object = s3d.Object('fureverdump', filename)
    s3object.put(Body=(bytes(json.dumps(expected_output).encode('UTF-8'))))

    print("done")


#key = '2020-01-29_10573.json'
#generate(key, 2)



list_o_files = ['2020-01-29_10573.json', '2020-01-29_10526.json',
                '2020-01-17_97002.json',  '2020-01-17_97003.json',
                '2020-01-17_97004.json', '2020-01-17_97005.json',
                '2020-01-18_97076.json', '2020-01-18_97075.json',
                '2020-01-17_97005.json', '2020-01-29_11752.json',
                '2020-01-29_11224.json', '2020-01-20_97124.json',
                '2020-01-29_97229.json', '2020-01-28_97229.json',
                '2020-01-21_97124.json', '2020-01-18_97007.json',
                '2020-01-29_11385.json', '2020-01-20_97123.json',
                '2020-01-20_97113.json', '2020-01-21_97123.json',
                '2020-01-29_11752.json', '2020-01-29_11778.json',
                '2020-01-29_12224.json', '2020-01-29_12180.json',
                '2020-01-29_11224.json', '2020-01-29_11782.json',
                '2020-01-29_11752.json', '2020-01-29_11718.json',
                '2020-01-29_11385.json', '2020-01-29_11778.json',
                '2020-01-29_10526.json', '2020-01-29_12067.json',
                '2020-01-29_10573.json', '2020-01-29_11520.json',
                '2020-01-29_97058.json', '2020-01-29_97502.json',
                '2020-01-29_97402.json', '2020-01-29_97138.json',
                '2020-01-29_97031.json', '2020-01-29_97321.json',
                '2020-01-29_97030.json', '2020-01-29_97601.json',
                '2020-01-29_97219.json', '2020-01-29_97603.json',
                '2020-01-29_10526.json', '2020-01-18_97008.json',
                '2020-01-18_97078.json', '2020-01-18_97086.json',
                '2020-01-20_97080.json', '2020-01-18_97077.json',
                '2020-01-18_97107.json', '2020-01-20_97107.json',
                '2020-01-20_97113.json', '2020-01-20_97123.json',
                '2020-01-20_97132.json', '2020-01-20_97124.json',
                '2020-01-20_97134.json', '2020-01-20_97140.json']


second_list = []
for i in range(400):
    filename = 'expected_output_' + str(i) + '.json'
    second_list.append(filename)
new_list = list_o_files + second_list


for num, file in enumerate(list_o_files, start = 5317):
    print(num)
    print(file)
    generate(file, num)