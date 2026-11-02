from collections.abc import Sequence

from parameter_server.types import KeyRange
import numpy as np


class WorkloadGenerator:
    def __init__(self, config):
        # Set randomisation seed
        self.rng = np.random.default_rng(seed=config["seed"])

        # Config for sample compute_ms
        self.worker_compute_ms_dist = config["workers"]["compute_ms"]["dist"]
        self.worker_compute_ms_mu = np.log(config["workers"]["compute_ms"]["median"])
        self.worker_compute_ms_sigma = config["workers"]["compute_ms"]["sigma"]
        self.worker_straggler_fraction = config["workers"]["stragglers"]["fraction"] 
        self.worker_stragglers_slowdown = config["workers"]["stragglers"]["slowdown"]

        # Config for sample keys
        self.access_mode = config["access"]["mode"]
        self.model_num_keys = config["model"]["num_keys"]
        ## Sparse mode: sample keys from a Zipf distribution
        self.access_keys_per_iter = config["access"]["keys_per_iter"]
        self.access_zipf_s = config["access"].get("zipf_s", None)
        self.access_scatter = config["access"]["scatter"]
        ## Dense mode: sample keys from a range of keys
        self. access_range_size = config["access"].get("range_size", None)
 
        # Load config for model simulation logic, assuming normally distributed noise for now, can be extended to other distributions later
        self.model_target_std = config["model"]["target"]["std"]
        self.model_target_mean = config["model"]["target"]["mean"]
        self.model_num_keys = config["model"]["num_keys"]

        # Config for gradient noise
        self.gradient_noise_std = config["gradient"]["noise_std"]

    def get_n_zipf_keys(self, n):
        return self.rng.zipf(a=self.access_zipf_s, size=n)

    def sample_keys(self) -> KeyRange | np.ndarray:
        match self.access_mode:
            case "sparse":
                if self.access_scatter:
                    keys = self.rng.choice(self.model_num_keys, size=self.access_keys_per_iter, replace=True)
                else:
                    # draw n ranks with replacement
                    keys = self.rng.zipf(a=self.access_zipf_s, size=self.access_keys_per_iter)
                # sample ensuring unique and sorted keys
                return  np.sort(np.unique(keys))
            case _: # dense and catchall
                start_key = self.rng.integers(0, self.model_num_keys - self.access_range_size)
                return KeyRange(start=start_key, end=start_key + self.access_range_size)
        
    def sample_compute_ms(self):
        # Sample compute_ms based on distribution specified in config
        match self.worker_compute_ms_dist:
            case "lognormal": # 
                compute_ms = self.rng.lognormal(mean=self.worker_compute_ms_mu, sigma=self.worker_compute_ms_sigma)/100
        # Straggler check
        if self.rng.random() < self.worker_straggler_fraction:
            compute_ms = compute_ms * self.worker_stragglers_slowdown
        return compute_ms    

    def sample_gradient_noise(self,size):
        return self.rng.normal(loc=0.0, scale=self.gradient_noise_std, size=size)

    def generate_target(self):
        # Hardcoded for now to assume:
        ## - dist: normal 
        ## - value_width 1
        return self.rng.normal(loc=self.model_target_mean, scale=self.model_target_std, size=self.model_num_keys)