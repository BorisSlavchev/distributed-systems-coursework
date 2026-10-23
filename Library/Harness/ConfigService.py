import yaml
import pprint

class ConfigService:

    def __init__(self, config_path):
        self.config_path = config_path
        self.config = {}
        self.importConfig()


    def importConfig(self):
        with open(self.config_path) as f:
            self.config = yaml.safe_load(f)

    def printConfig(self):
        pprint.pprint(self.config)

