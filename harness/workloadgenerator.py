from parameter_server import types
import numpy as np
import random


class WorkloadGenerator:
    def __init__(self, config):
        self.config = config
        # Set randomisation seed
        # random.seed(config["seed"])
        self.keys_rng=np.random.default_rng()
        # Load config for keys generation logic
        self.access_mode = config["access"]["mode"]
        self.access_scatter = config["access"]["scatter"]
        self.access_keys_per_iter = config["access"]["keys_per_iter"]
        # Handling range_size as a optional config
        self.access_range_size = config["access"].get("range_size", None)
       # hardcoded to assume zipf distribution for now, can be extended to other distributions later
        self.keys_a = config["access"]["zipf_s"]
        # Load config for worker simulation logic
        self.worker_straggler_fraction = config["workers"]["stragglers"]["fraction"] 
        self.worker_compute_median = config["workers"]["compute_ms"]["median"]
        self.worker_compute_mu = np.log(self.worker_compute_median)
        self.worker_compute_sigma = config["workers"]["compute_ms"]["sigma"]
        self.worker_stragglers_slowdown = config["workers"]["stragglers"]["slowdown"]


    def sample_compute_ms(self):
        # Sample compute_ms based on lognormal distribution * straggler factor (if applicable)
        ## hardcorded to assume lognormal distribution for now, can be extended to other distributions later
        compute_ms = np.random.lognormal(mean=self.worker_compute_mu, sigma=self.worker_compute_sigma)/100
        # Straggler check
        straggler_rng = random.random()
        is_straggler = straggler_rng  < self.worker_straggler_fraction
        if is_straggler:
            compute_ms = compute_ms * self.worker_stragglers_slowdown
        return compute_ms    

    def sample_keys(self):
        ### TODO: revisit what Scatter mode does
        if self.access_scatter:
            # ensure keys drawn are unique and sorted
            keys = np.sort(np.unique(self.keys_rng.zipf(a=self.keys_a, size=self.access_keys_per_iter)))
        else:
            keys =[i for i in range(1, self.access_keys_per_iter+1)]
        ### TODO: revisit what Dense mode means
        if self.access_mode == "dense":
            keys=types.KeyRange(keys[0],keys[-1])
        return keys


