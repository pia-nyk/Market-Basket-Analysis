import time

from pyspark import SparkContext
import sys
import csv
from python import get_frequent_items

CSV_FILE = "../customer_product.csv"

# create spark connection object
def conf_spark():
    sc = SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    return sc

#preprocess each row of ta feng
def format_row(data):
    parts = data.split(",")
    date = parts[0].strip('"')
    cust_id = parts[1].strip('"')
    prod_id = parts[-4].strip('"')
    date_cust_id = str(date) + "-" + str(cust_id)
    return date_cust_id, int(prod_id.lstrip('0'))

#get processed ta feng data
def process_ta_feng_dataset(sc, file):
    rdd = sc.textFile(file)
    tagsheader = rdd.first()
    data = rdd.filter(lambda row: row != tagsheader).map(lambda row: format_row(row)).collect()
    return data

#save the processed data to csv for next task
def write_processed_data_as_csv(processed_data):
    try:
        with open("../customer_product.csv", 'w') as fw:
            csv_out = csv.writer(fw)
            csv_out.writerow(["DATE-CUSTOMER_ID", "PRODUCT_ID"])
            for row in processed_data:
                csv_out.writerow(row)
        return True
    except:
        return False

if __name__ == "__main__":
    sc = conf_spark()
    processed_data = process_ta_feng_dataset(sc, sys.argv[3])
    is_file_creation_success = write_processed_data_as_csv(processed_data)
    filter_criteria = int(sys.argv[1])

    start = time.time()
    rdd = sc.textFile(CSV_FILE)
    tagsheader = rdd.first()
    baskets = rdd.filter(lambda row: row != tagsheader).map(lambda row: (row.split(",")[0], row.split(",")[1])).groupByKey().map(lambda row: sorted(set(row[1]))).filter(lambda row: len(row) > filter_criteria)
    get_frequent_items.run_SON(baskets, sys.argv[4], sys.argv[2])
    finish = time.time() - start
    print("Duration: " + str(finish))
