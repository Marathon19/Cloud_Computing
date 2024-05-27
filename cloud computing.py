import csv
from collections import defaultdict
from multiprocessing import Pool

def map_passenger_flights(filename):
    passenger_flights = defaultdict(int)
    with open(filename, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            passenger_id = row[0]
            passenger_flights[passenger_id] += 1
    return passenger_flights

def reduce_passenger_flights(results):
    total_flights = defaultdict(int)
    for result in results:
        for passenger_id, num_flights in result.items():
            total_flights[passenger_id] += num_flights
    return total_flights

def find_passenger_with_most_flights(passenger_flights):
    max_passenger = max(passenger_flights, key=passenger_flights.get)
    max_flights = passenger_flights[max_passenger]
    return max_passenger, max_flights

if __name__ == '__main__':
    data_files = ['data/AComp_Passenger_data_no_error.csv'] # 添加更多文件

    pool = Pool(processes=len(data_files))
    results = pool.map(map_passenger_flights, data_files)
    pool.close()
    pool.join()

    total_passenger_flights = reduce_passenger_flights(results)

    max_passenger, max_flights = find_passenger_with_most_flights(total_passenger_flights)

    print(f"The passenger ID with the most flights is: {max_passenger}，The number of flights is: {max_flights}")