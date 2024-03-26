import os
import yaml
import logging

from nb2workflow import conf_dir

logger = logging.getLogger("conf")


class ConfigEnv(object):

    def __init__(self, cfg_dict):
        self.cfg_dict = cfg_dict

        if 'service' in self.cfg_dict.keys():
            service_dict = cfg_dict['service']

            self.set_service_conf(service_dict.get('max_download_size', 1000000000))

    @classmethod
    def from_conf_file(cls, conf_file_path, set_by=None):

        if conf_file_path is None:
            conf_file_path = os.path.join(conf_dir, 'service_conf.yml')
            logger.info(f"using conf file from default dir {conf_file_path}")
        else:
            logger.info(f"loading config from the file: {conf_file_path}")

        with open(conf_file_path, 'r') as conf_file:
            cfg_dict = yaml.load(conf_file, Loader=yaml.SafeLoader)

        logger.debug('cfg_dict: %s', cfg_dict)
        return ConfigEnv(cfg_dict)

    def set_service_conf(self,
                         max_download_size):
        self.max_download_size = max_download_size
