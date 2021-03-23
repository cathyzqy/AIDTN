from datetime import datetime
import requests
import time
import pandas as pd
import numpy
import random
import os


def finish_transfer(transfer_id, orchestrator, sender, receiver):
    response = requests.post('{}/wait/{}'.format(orchestrator, transfer_id))
    result = response.json()

    cleanup(sender, receiver)


def cleanup(sender, receiver, retry=5):
    for i in range(0, retry):
        response = requests.get('{}/cleanup/nuttcp'.format(sender))
        if response.status_code != 200:
            continue
        response = requests.get('{}/cleanup/nuttcp'.format(receiver))
        if response.status_code != 200:
            continue

        response = requests.get('{}/cleanup/stress'.format(sender))
        response = requests.get('{}/cleanup/stress'.format(receiver))
        response = requests.get('{}/cleanup/fio'.format(sender))
        response = requests.get('{}/cleanup/fio'.format(receiver))

        return
    raise Exception('Cannot cleanup after %s tries' % retry)


def get_transfer(transfer_id, orchestrator):
    response = requests.get('{}/transfer/{}'.format(orchestrator, transfer_id))
    result = response.json()
    print(result)


def parse_nvme_usage(filename):
    df = pd.read_csv(filename, parse_dates=[0])
    df['elapsed'] = (df['Time'] - df['Time'][0]) / numpy.timedelta64(1, 's')

    df = df[['elapsed', 'written_mean']].astype('int32').set_index('elapsed')
    df['written_mean'] = df['written_mean'].apply(str) + 'M'

    return df.to_dict()['written_mean']


def prepare_transfer(srcdir, sender, receiver):
    result = requests.get('{}/files/{}'.format(sender, srcdir))
    files = result.json()
    file_list = [srcdir + i['name'] for i in files if i['type'] == 'file']
    dirs = [srcdir + i['name'] for i in files if i['type'] == 'dir']

    response = requests.post('{}/create_dir/'.format(receiver), json=dirs)
    if response.status_code != 200: raise Exception('failed to create dirs')
    return file_list


def start_transfer(file_list, num_workers, orchestrator, sender_id, receiver_id, duration=5):
    data = {
        'srcfile': file_list,
        'dstfile': file_list,  # ['/dev/null'] * len(file_list),
        'num_workers': num_workers,
        'iomode': 'read',
        'blocksize': 1024
    }

    response = requests.post('{}/transfer/fio/{}/{}'.format(orchestrator, sender_id, receiver_id),
                             json=data)  # error out
    result = response.json()
    assert result['result'] is True
    transfer_id = result['transfer']
    return transfer_id


def start_transfer_mrp(file_list, num_workers, orchestrator, duration=5):
    data = {
        'srcfile': file_list,
        'dstfile': file_list,  # ['/dev/null'] * len(file_list),
        'num_workers': num_workers,
        'duration': duration  # ,
        # 'blocksize' : 8192
    }

    response = requests.post('{}/transfer/nuttcp/2/1'.format(orchestrator), json=data)  # error out
    result = response.json()
    assert result['result'] == True
    transfer_id = result['transfer']
    return transfer_id


def start_nvme_usage(nvme_usage, sender):
    data = {
        'sequence': nvme_usage,
        'file': 'disk0/fiotest',
        'size': '1G',
        'address': ''
    }
    response = requests.post('{}/receiver/stress'.format(sender), json=data)
    result = response.json()
    assert result.pop('result') is True


def wait_for_transfer(transfer_id, orchestrator, sender):
    while True:
        response = requests.get('{}/check/{}'.format(orchestrator, transfer_id))
        result = response.json()
        if result['Unfinished'] == 0:
            response = requests.get('{}/cleanup/stress'.format(sender))
            break
        time.sleep(30)


def static_transfer(num_workers, sender_id, receiver_id, cluster):
    nvme_usage = parse_nvme_usage('nvme_usage_daily.csv')
    start_nvme_usage(nvme_usage, sender)
    sender_obj = requests.get('{}/DTN/{}'.format(orchestrator, 2)).json()
    receiver_obj = requests.get('{}/DTN/{}'.format(orchestrator, 1)).json()

    file_list = prepare_transfer(srcdir, sender, receiver)
    if cluster == 'mrp_nvmeof':
        transfer_id = start_transfer(file_list, num_workers, orchestrator, sender_id, receiver_id)
    else:
        transfer_id = start_transfer_mrp(file_list, num_workers, orchestrator)

    start_time = datetime.now()
    print('transfer_id %s , start_time %s' % (transfer_id, start_time))

    wait_for_transfer(transfer_id, orchestrator, sender)

    return transfer_id


if __name__ == "__main__":

    cluster = 'mrp_nvmeof'

    if cluster == 'prp':
        orchestrator = 'https://dtn-orchestrator-2.nautilus.optiputer.net'
        sender = 'http://dtn-sender-2.nautilus.optiputer.net'
        receiver = 'http://dtn-receiver-2.nautilus.optiputer.net'
        srcdir = 'project/'
        sender_instance = 'siderea.ucsc.edu'
        receiver_instance = 'k8s-nvme-01.ultralight.org'
        monitor = 'https://thanos.nautilus.optiputer.net'
        sender_id = 2
        receiver_id = 1
    elif cluster == 'mrp':
        orchestrator = 'http://dtn-orchestrator.starlight.northwestern.edu'
        sender = 'http://dtn-sender.starlight.northwestern.edu'
        receiver = 'http://dtn-receiver.starlight.northwestern.edu'
        srcdir = 'project/'
        sender_instance = '165.124.33.175:9100'
        receiver_instance = '131.193.183.248:9100'
        monitor = 'http://165.124.33.158:9091/'
        sender_id = 2
        receiver_id = 1
    elif cluster == 'mrp_nvmeof':
        orchestrator = 'http://dtn-orchestrator.starlight.northwestern.edu'
        sender = 'http://dtn-sender.starlight.northwestern.edu'
        receiver = 'http://dtn-receiver-nvmeof.starlight.northwestern.edu'
        srcdir = 'project/'
        sender_instance = '165.124.33.175:9100'
        receiver_instance = '131.193.183.248:9100'
        monitor = 'http://165.124.33.158:9091/'
        sender_id = 2
        receiver_id = 4

    cleanup(sender, receiver)
    # generate different num_workers data
    for num_workers in range(1, 100):
        transfer_id = static_transfer(num_workers, sender_id, receiver_id, cluster)
        finish_transfer(transfer_id, orchestrator, sender, receiver)
        get_transfer(transfer_id, orchestrator)
