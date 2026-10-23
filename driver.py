from Library.Harness.ConfigService import ConfigService


def main(config_path):
    # initializing driver with config
    config = ConfigService(config_path)
    config.printConfig()

if __name__ == "__main__":
    config_config_path = "Test/Harness/Config/zipfboundeddelay.yaml"
    main(config_config_path)
