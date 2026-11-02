import time

import yaml
import pprint
import uuid
import threading
from harness.observer import Observer
from harness.workloadgenerator import WorkloadGenerator
from harness.workeremulator import WorkerEmulator
from harness.observer import Observer
from parameter_server import client
from parameter_server import types

# from parameter_server import Server

class Driver:
    def __init__(self, config_path):
        # Load config from yaml
        self.config = self.read_config(config_path)

        # Intialize harness components
        self.wg = WorkloadGenerator(self.config)

        # Generating target model for convergence check
        self.w_star = self.wg.generate_target()
        
        # Config for senario       
        self.time_scenario = {}
        self.iteration_scenario ={}
        self.parse_scenarios()

        self.workers = {}
        self.servers = {}

        # Config for server
        self.num_servers = self.config["cluster"]["servers"]
        self.replication = self.config["cluster"]["replication"]

        # Config for workers
        self.num_workers = self.config["workers"]["count"]

        self.start_time = time.time_ns()
        self.initialize_servers()
        # Configure observer 
        self.observer = Observer(self.config, self.servers, self.w_star, self.start_time)
        self.observer.start()
        # self.schedule_time_scenario()
        self.initialize_workers()

        self.observer.stop()

    
        
    def read_config(self, config_path):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config

    def initialize_servers(self):
        for i in range(self.num_servers):
            self.add_server(self.replication)

    def add_server(self, replication):
        next_id = self.servers.keys() and max(self.servers.keys()) + 1 or 0
        ### TODO @Boris how to launch server?
        self.servers[next_id] = None

    def initialize_workers(self):
        for i in range(self.num_workers):
            self.add_worker()

    def add_worker(self):
        ### TODO: @Boris how to launch worker? also where is the struct for call options
        
        next_id = self.workers.keys() and max(self.workers.keys()) + 1 or 0
        # client = Client.connect(address, worker_id=worker_id, opts=None) 
        client = None
        self.workers[next_id] = WorkerEmulator(self.config, self.wg, client, self.w_star)
        self.workers[next_id].run()

    def kill_worker(self, target):
        ### TODO
        print("killing worker", target)
        
    def kill_server(self, target):
        ### TODO
        print("killing server ", target)

    def remove_server(self, target):
        ### TODO
        print("remove server ", target)

    def delay_link(self, a, b, jitter_ms):
        ### TODO
        print("delay link ", a, b, jitter_ms)

    def drop_link(self, a, b, p):
        ### TODO
        # drop ink with propability p
        print("drop link ", a, b, p)

    def partition(self, groups, duration_s):
            ### TODO
            print("partition ", groups, duration_s)

        
    def parse_scenarios(self):
        for scenario in self.config["scenario"]:
            trigger = scenario["at"]
            if "time_s" in trigger:
                if trigger["time_s"] not in self.time_scenario:
                    self.time_scenario[trigger["time_s"]] = []
                self.time_scenario[trigger["time_s"]].append(scenario)
            elif "iteration" in trigger:
                if trigger["iteration"] not in self.iteration_scenario:
                    self.iteration_scenario[trigger["iteration"]] = []
                self.iteration_scenario[trigger["iteration"]].append(scenario)

    def schedule_time_scenario(self):
        for event_time in self.time_scenario:
            for scenario in self.time_scenario[event_time]:
                scenario["timer"]= threading.Timer(event_time,self.execute_scenario,args=[scenario])
                scenario["timer"].start()

    def execute_scenario(self, scenario):
        match scenario["action"]:
            case "kill_server":
                self.kill_server(scenario["target"])
            case "kill_worker":
                self.kill_worker(scenario["target"])
            case "add_server":
                self.add_server(scenario.get("count",1))
            case "add_worker":
                self.add_worker(scenario.get("count",1))
            case "remove_server":
                self.remove_server(scenario["target"])
            case "delay_link":
                self.delay_link(scenario.get("a"), scenario.get("b"), scenario.get("jitter_ms"))
            case "drop_link":
                self.drop_link(scenario.get("a"), scenario.get("b"), scenario.get("p"))

if __name__ == "__main__":
    # CONFIG_PATH = "config/zipfboundeddelay.yaml"
    CONFIG_PATH = "config/singleworkersgd.yaml"
    driver = Driver(CONFIG_PATH)