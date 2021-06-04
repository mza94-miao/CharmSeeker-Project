import boto3
import botocore
import base64
import math
import numpy
from multiprocessing import Process, Queue
from multiprocessing.pool import ThreadPool

lambda_config = botocore.config.Config(connect_timeout=900, read_timeout=900, retries={'max_attempts': 0})
client = boto3.client('lambda', config=lambda_config)
function1_name = 'pipe-decoder-p'
function2_name = 'pipe-yolo-p'
time_out = 900
total_workloads = [390, 1950]


def invoke_function_once(function_name, payload, memory_size):
    invoke_res = client.invoke(FunctionName=function_name, Payload=payload, LogType='Tail')
    if invoke_res['ResponseMetadata']['HTTPStatusCode'] == 200:
        log_res = base64.b64decode(invoke_res['LogResult']).split('\n')[2].split('\t')
        duration = float(log_res[1].split(' ')[1])
        billed_duration = math.ceil(duration)
        cost = 0.0000166667 / 1024 / 1000 * memory_size * billed_duration + 0.0000002
        return duration / 1000, cost


def get_value_1(memory, workload, q):
    config_res = client.update_function_configuration(FunctionName=function1_name, Timeout=time_out, MemorySize=memory)
    invoke_numbers = math.ceil(total_workloads[0] / workload)
    pool = ThreadPool(invoke_numbers)
    durations = []
    costs = []
    results = []

    if config_res['ResponseMetadata']['HTTPStatusCode'] == 200:
        for i in range(invoke_numbers):
            start = i * workload
            end = (i + 1) * workload - 1
            pay_load = f'{"start": {start},"end": {end}}'
            results.append(pool.apply_async(invoke_function_once, (function1_name, pay_load, memory)))

        results = [r.get() for r in results]
        for i in results:
            durations.append(i[0])
            costs.append(i[1])

        total_cost = numpy.sum(costs)
        max_duration = numpy.max(durations)
        q.put((max_duration, total_cost))
    else:
        q.put((time_out, (0.0000166667 / 1024 * memory * time_out + 0.0000002) * invoke_numbers))


def get_value_2(memory, workload, q):
    config_res = client.update_function_configuration(FunctionName=function2_name, Timeout=time_out, MemorySize=memory)
    invoke_numbers = math.ceil(total_workloads[1] / workload)
    pool = ThreadPool(invoke_numbers)
    durations = []
    costs = []
    results = []

    if config_res['ResponseMetadata']['HTTPStatusCode'] == 200:
        for i in range(invoke_numbers):
            start = i * workload + 1
            end = (i + 1) * workload
            pay_load = f'{"start": {start},"end": {end}}'
            results.append(pool.apply_async(invoke_function_once, (function2_name, pay_load, memory)))

        results = [r.get() for r in results]
        for i in results:
            durations.append(i[0])
            costs.append(i[1])

        total_cost = numpy.sum(costs)
        max_duration = numpy.max(durations)
        q.put((max_duration, total_cost))
    else:
        q.put((time_out, (0.0000166667 / 1024 * memory * time_out + 0.0000002) * invoke_numbers))


def compute_values(config):
    q1 = Queue()
    proc1 = Process(target=get_value_1, args=(config[0], config[1], q1))
    proc1.start()

    q2 = Queue()
    proc2 = Process(target=get_value_2, args=(config[2], config[3], q2))
    proc2.start()

    proc1.join()
    proc2.join()

    stage1_res = q1.get()
    stage2_res = q2.get()

    duration = stage1_res[0] + stage2_res[0]
    cost = stage1_res[1] + stage2_res[1]

    print(f"log duration: {math.log(duration)}, log costs: {math.log(cost)}")
    return math.log(duration), math.log(cost)


def main(job_id, params):
    print(f"Job id {job_id} enter CPS 2-stage pipeline main function")
    config = [params['memory_1'][0] * 64, int(math.pow(2, params['workload_1'][0])), params['memory_2'][0] * 64,
              int(math.pow(2, params['workload_2'][0]))]
    return compute_values(config)
