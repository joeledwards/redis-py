import readline
import os

class Config(object):
    def __init__(self, config_file):
        object.__init__(self)
        self.data = {
            "host" : None,
            "port" : None,
            "auth" : None,
        }
        self.parse(config_file)

    # Map Overrides
    def __len__(self):
        return len(self.data)

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value

    def __delitem__(self, key):
        del self.data[key]

    def __iter__(self):
        return self.data.iterkeys()

    def __reversed__(self):
        return reversed(self.data)

    def __contains__(self, key):
        return self.data.has_key(key)

    def has_key(self, key):
        return self.data.has_key(key)

    def keys(self):
        return self.data.keys()

    def parse(self, config_file):
        lines = open(config_file, 'r').readlines()
        for line in lines:
            if line.strip().startswith('#'):
                continue
            key,value = map(lambda l: l.strip(), line.split('=',1))
            if not self.data.has_key(key):
                raise KeyError(key)
            self.data[key] = value

def from_file(config_file):
    return Config(config_file)

def find_configs():
    config_dir = "%(HOME)s/.ssh" % os.environ
    config_files = []
    for file in os.listdir(config_dir):
        if not file.endswith('.redis'):
            continue
        path = "%s/%s" % (config_dir, file)
        if not os.path.isfile(path):
            continue
        config_files.append(path)
    return sorted(config_files)

def select_config(config_files):
    file_list = sorted(config_files)
    file_count = len(file_list)
    config_file = None
    while config_file is None:
        try:
            if file_count > 0:
                print "Configuration Files:"
                for i in range(0, file_count):
                    print "  %d : %s" % (i, file_list[i])

            selection = raw_input("enter selection: ")

            try:
                config_file = file_list[int(selection)]
            except:
                config_file = selection

            if not os.path.isfile(config_file):
                config_file = None

        except:
            config_file = None

    return config_file

