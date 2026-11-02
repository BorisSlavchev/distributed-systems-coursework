import time
import asyncio

import numpy as np
import json

class Observer:
    def __init__(self, config, servers, w_star, start_time):
        self.config = config
        self.servers = servers
        self.w_star = w_star
        self.start_time = start_time
        self._task = None
        self.is_running = False
        # Loading config for observer
        self.output_dir = config["output"]["dir"]
        self.observer_every = config[""]["output"]["observer_every"]
        # Config for convergence check
        self.convergence_filename = "convergence.json"
        self.convergence_log = []
        self.start()


    def start(self):
        """Starts the background loop safely."""
        if not self.is_running:
            self.is_running = True
            self._task = asyncio.create_task(self._run_loop())
            print("Background worker started.")

    def stop(self):
        """Gracefully stops the background loop."""
        if self.is_running:
            self.is_running = False
            if self._task:
                self._task.cancel()
            # writing all the logs to file
            with open(f"{self.output_dir}/{self.convergence_filename}", "w") as f:
                json.dump(self.convergence_log, f, indent=4)

    async def _run_loop(self):
        while self.is_running:
            start_time = asyncio.get_event_loop().time()

            # Perform Observer checks
            await self.convergence_check()

            # Correct for execution drift
            exec_time = asyncio.get_event_loop().time() - start_time
            sleep_time = max(0, self.observer_every - exec_time)
            
            try:
                await asyncio.sleep(sleep_time)
            except asyncio.CancelledError:
                # Triggers when task.cancel() is called
                break

    async def convergence_check(self):
        Consolidated_weights = []
        iteration = 0
        for server in self.servers:
            ### TODO @Boris how to pull the weights from the server?
            Consolidated_weights.append(server.pull_weights())
            iteration = min(iteration, server.get_iteration())
        # measure ecuclidean distance between the consolidated weights and w_star
        euclidean_distance = np.linalg.norm(np.array(Consolidated_weights) - self.w_star)
        self.convergence_log.append({
            "iteration": iteration,
            "euclidean_distance": euclidean_distance,
            "timestamp": time.time_ns() - self.start_time
        })


    ### TODO: pull the whole model to measure convergence
     

    def record(self, iteration, hp, hq):
        pass