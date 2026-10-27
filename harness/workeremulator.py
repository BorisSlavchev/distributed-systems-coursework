from harness.workloadgenerator import WorkloadGenerator
from parameter_server import Client
import time

class WorkerEmulator:
    def __init__(self, config, wg:WorkloadGenerator, client):
        self.config = config
        self.num_iteration = config["workers"]["iterations"]
        self.wg = wg
        self.consistency_mode = config["consistency"]["mode"]
        self.consistency_tau = config["consistency"]["tau"]
        self.client = client

    def run(self):
        for t in self.num_iteration:
            K = self.wg.sample_keys()
            self.wait_for_bound(t - self.consistency_tau -1)
            hp = self.client.pull(K, iteration=t)
            W = hp.wait()
            time.sleep(self.wg.sample_compute_ms())
            # G = (W - w_star[K]) + N(0, noise_std)
            # hq = client.push(K, G,iteration=t, deps=[hp] )
            # record(t, hp, hq)

    def wait_for_bound(self, j):
        if j >= 1:
            #TODO implement wait
            pass
