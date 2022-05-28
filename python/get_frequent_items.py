import itertools

from pyspark import SparkContext
import sys
import math
import time

# create spark connection object
def conf_spark():
    sc = SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    return sc

#baskets creation for case 1
def create_user_baskets(sc, file):
    rdd = sc.textFile(file)
    tagsheader = rdd.first()
    baskets = rdd.filter(lambda row: row != tagsheader).map(lambda row: (row.split(",")[0], row.split(",")[1])).groupByKey().map(lambda row: sorted(set(row[1])))
    return baskets

#baskets creation for case 2
def create_business_baskets(sc, file):
    rdd = sc.textFile(file)
    tagsheader = rdd.first()
    baskets = rdd.filter(lambda row: row != tagsheader).map(lambda row: (row.split(',')[1], row.split(',')[0])).groupByKey().map(lambda row: sorted(set(row[1])))
    return baskets

#get the bucket in which to hash the pair
def get_bucket(pair, bucket_size):
    item1 = int(pair[0])
    item2 = int(pair[1])
    return (item1 + item2) % bucket_size

#check if the current pairs of size n each can be used to create item of size n+1
def is_valid_pair(pair):
    item1 = pair[0]
    item2 = pair[1]
    return item1[:-1] == item2[:-1]

#pass 1 of pcy
def first_pass(chunk, s, bucket_size):
    buckets = [0] * bucket_size
    count = {}
    for basket in chunk:
        for pair in itertools.combinations(basket, 2):
            hash = get_bucket(pair, bucket_size)
            buckets[hash] += 1
        for item in basket:
            if item not in count:
                count[item] = 0
            count[item] += 1

    bitmap = [1 if _ >= s else 0 for _ in buckets]
    freq_singletons = []
    for k,v in count.items():
        if v >= s:
            freq_singletons.append((k,))
    return freq_singletons, bitmap

#pass 2 of pcy
def second_pass(freq_singletons, bitmap, chunk, s, bucket_size):
    freq_pairs = []
    count = {}
    freq_singletons = list(set().union(_[0] for _ in freq_singletons))
    for basket in chunk:
        basket_candidates = list(set(basket) & set(freq_singletons))
        for pair in itertools.combinations(basket_candidates, 2):
            pair = tuple(sorted(pair))
            hash = get_bucket(pair, bucket_size)
            if bitmap[hash] == 1:
                if pair not in count:
                    count[pair] = 0
                count[pair] += 1
    for k,v in count.items():
        if v >= s:
            freq_pairs.append(k)
    return freq_pairs

#pass 3 onwards for pcy
def further_pass(freq_itemsets, chunk, s):
    itemsets = []
    for pair in itertools.combinations(freq_itemsets, 2):
        if is_valid_pair(pair):
            candidate = tuple(sorted(list(pair[0]) + list([pair[1][-1]])))
            # print(candidate)
            is_valid = True
            for item in itertools.combinations(candidate, len(candidate)-1):
               if item not in freq_itemsets:
                   is_valid = False
                   break
            if is_valid:
                itemsets.append(candidate)
    count = {}
    for basket in chunk:
        for item in itemsets:
            if set(item).issubset(tuple(basket)):
                if not item in count:
                    count[item] = 0
                count[item] += 1

    freq_itemsets_n = []
    for k,v in count.items():
        if v >= s:
            freq_itemsets_n.append(k)
    return freq_itemsets_n

#driver method for pcy
def PCY(chunk, support, total_baskets):
    chunk = list(chunk)
    ps = math.ceil(int(support) * len(chunk) / total_baskets)
    bucket_size = int(len(chunk) / ps)
    # print(ps)
    itemsets = {}
    itemsets[1], bitmap = first_pass(chunk, ps, bucket_size)
    itemsets[2] = second_pass(itemsets[1], bitmap, chunk, ps, bucket_size)
    local_itemsets = itemsets[2]
    # print(local_itemsets)
    pass_no = 3
    while len(local_itemsets) > 0:
        local_itemsets = further_pass(local_itemsets, chunk, ps)
        itemsets[pass_no] = local_itemsets
        pass_no += 1

    all_candidates = []
    for _ in range(1,pass_no):
        for v in itemsets[_]:
            all_candidates.append(v)
    yield all_candidates

#verify if the candidate itemsets found in pass 1 of son are actually frequent
def verify_itemsets(value, itemsets):
    count = {}
    for _ in itemsets:
        count[_] = 0
    for item in itemsets:
        if set(item).issubset(tuple(value)):
            count[item] += 1
    return count

#helper methods to merge 2 dicts obtained from map
def merge_two_dicts(a,b):
    count = {}
    for k,v in a.items():
        count[k] = v
    for k,v in b.items():
        if k in count:
            count[k] += v
        else:
            count[k] = v
    return count

#formating the data as dict
def create_dict(int_list):
    # print(int_list)
    fin_dict = {}
    for _ in int_list:
        if isinstance(_, str):
            if 1 not in fin_dict:
                fin_dict[1] = []
            fin_dict[1].append(_)
        else:
            # print(_)
            if len(_) not in fin_dict:
                fin_dict[len(_)] = []
            local = []
            for tp in _:
                local.append(tp)
            fin_dict[len(_)].append(tuple(sorted(local)))
    return fin_dict

#write results to file
def write_to_file(dict, erase, output_file):
    mode = 'a'
    title = "Frequent Itemsets:"
    if erase:
        mode = 'w'
        title = "Candidates:"
    with open(output_file, mode) as fw:
        fw.write(str(title) + "\n")
        max_val = max(dict.keys())
        for _ in range(1, max_val+1):
            v = sorted(dict[_])
            if _ == 1:
                fw.write("(\'" + str(v[0][0]) + "\')")
                for _ in range(1, len(v)):
                    fw.write(",(\'" + str(v[_][0]) + "\')")
            else:
                fw.write(str(v[0]))
                for _ in range(1, len(v)):
                    fw.write("," + str(v[_]))
            fw.write("\n\n")

#driver method for SON algo
#todo: pass support from method signature instead of reading from system
def run_SON(baskets, output_file, support):
    # total no. of baskets
    total_baskets = baskets.count()

    data = baskets.mapPartitions(lambda data: PCY(data, support, total_baskets)).flatMap(
        lambda itemsets: itemsets).distinct().sortBy(lambda data: (len(data), data)).collect()
    # print(data)
    int_dict = create_dict(data)
    write_to_file(int_dict, True, output_file)

    verified_itemsets = baskets.map(lambda value: verify_itemsets(value, data)).reduce(
        lambda a, b: merge_two_dicts(a, b))
    delete = [k for k in verified_itemsets if verified_itemsets[k] < int(support)]
    for _ in delete: del verified_itemsets[_]

    # print(list(verified_itemsets.keys()))
    print(len(verified_itemsets.keys()))
    fin_dict = create_dict(verified_itemsets.keys())
    write_to_file(fin_dict, False, output_file)


if __name__ == '__main__':
    start = time.time()
    sc = conf_spark()
    text_file = sys.argv[3]
    if int(sys.argv[1]) == 1:
        baskets = create_user_baskets(sc, text_file)
    else:
        baskets = create_business_baskets(sc, text_file)
    # print(baskets.collect())
    run_SON(baskets, sys.argv[4], sys.argv[2])
    finish = time.time() - start
    print("Duration: " + str(finish))
