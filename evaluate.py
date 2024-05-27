import csv
from collections import defaultdict
from multiprocessing import Pool

def map_function(filename):
    passenger_flights = defaultdict(int)
    with open(filename, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            passenger_id = row[0]
            passenger_flights[passenger_id] += 1
    return passenger_flights

def reduce_function(all_flights, file_flights):
    for passenger_id, num_flights in file_flights.items():
        all_flights[passenger_id] += num_flights
    return all_flights

def find_max_passenger(passenger_flights):
    return max(passenger_flights, key=passenger_flights.get), passenger_flights[max(passenger_flights, key=passenger_flights.get)]

def process_data():
    data_files = ['data/AComp_Passenger_data.csv', 'data/AComp_Passenger_data_no_error_DateTime.csv']

    # 使用多进程池来并行处理数据文件
    with Pool(processes=len(data_files)) as pool:
        results = pool.map(map_function, data_files)

    # 初始化一个空的defaultdict来存储所有文件的总航班数
    all_flights = defaultdict(int)

    # 合并所有文件的结果
    for file_flights in results:
        all_flights = reduce_function(all_flights, file_flights)

    # 为每个文件单独找到拥有最多航班的乘客
    for i, file_flights in enumerate(results):
        file_max_passenger, file_max_flights = find_max_passenger(file_flights)
        print(f"The passenger ID with the most flights in file {data_files[i]} is {file_max_passenger}, and the number of flights is: {file_max_flights}.")

if __name__ == '__main__':
    process_data()
