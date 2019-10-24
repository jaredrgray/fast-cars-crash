#!/usr/bin/env python3

import random
import uuid

HASHTAGS = ['#one', '#two', '#three', '#four', '#five', '#six', '#seven',
        '#eight', '#nine', '#ten']

def new_sample(num_partitions):
    timestamp = random.randrange(1, 1000000)
    partition_no = random.randrange(1, num_partitions + 1)
    asset_id = uuid.uuid4()
    num_hashtags = random.randrange(1, len(HASHTAGS))
    hashtags  = ','.join(random.choices(HASHTAGS, k=num_hashtags))
    return ','.join((str(timestamp), str(partition_no), str(asset_id), hashtags))

def main():
    with open('input_samples.txt', 'wt') as f:
        num_partitions = 20
        num_samples = 150000
        for i in range(1, num_samples):
            f.write(new_sample(num_partitions))
            f.write('\n')
        f.write(new_sample(num_partitions))

if __name__ == '__main__':
    main()
