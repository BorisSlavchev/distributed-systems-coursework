from harness.observer import Observer
from harness.workloadgenerator import WorkloadGenerator
from parameter_server.types import UINT32_MAX, UINT64_MAX, Ack, KeyRange, TaskId, Values, unsigned
from parameter_server import Client
import time

class WorkerEmulator:
    def __init__(self, config, wg:WorkloadGenerator, o:Observer, client, w_star):
        self.num_iteration = config["workers"]["iterations"]
        self.wg = wg
        self.o = o
        self.client = client
        # Consistency config
        self.consistency_mode = config["consistency"]["mode"]
        self.consistency_tau = config["consistency"]["tau"]
        # Gradient config
        self.gradient_type = config["gradient"]["type"]
        self.gradient_noise_std = config["gradient"]["noise_std"]
        self.w_star = w_star
        self.model_learning_rate = config["model"]["learning_rate"]
        
    def run(self):
        for t in range(self.num_iteration):
            K = self.wg.sample_keys()
            self.wait_for_bound(self.client, t - self.consistency_tau -1)
            # TODO @Boris where how do i configure the call option?
            hp = self.client.pull(K, iteration=t) #  Sequence[int] | KeyRange
            W = hp.wait() # <- what is the result format?
            time.sleep(self.wg.sample_compute_ms())
            match self.gradient_type:
                case "target_diff":
                    G = W - (self.w_star[K]) * self.model_learning_rate
                case "fixed":
                    if isinstance(K, KeyRange):
                        size = K.end-K.start
                    else:
                        size = K.size
                    G = W - (self.w_star[K] + self.wg.sample_gradient_noise(size)) * self.model_learning_rate
            hq = self.client.push(K, G,iteration=t, deps=[hp])
            self.o.record(t, hp, hq) # Vector clock record?

    def wait_for_bound(self, client, j):
        if j >= 1:
            #TODO implement how to know if the server has been done
            pass
