import os
import time
import warnings

import yaml
from munch import munchify, unmunchify, Munch

ENV_VAR_ROOT: str = "QSTRADER"
DEFAULT_CONFIG_FILENAME: str = "~/qstrader.yml"


def from_env(key: str, default_value: str = None, root: str = ENV_VAR_ROOT) -> str:
    """Returns a value (url, login, password)
    using either default_value or using environment variable"""
    if root != "":
        ENV_VAR_KEY: str = root + "_" + key.upper()
    else:
        ENV_VAR_KEY: str = key.upper()
    if default_value == "" or default_value is None:
        try:
            return os.environ[ENV_VAR_KEY]
        except Exception:
            warnings.warn(
                "You should pass %s using --%s or using environment variable %r"
                % (key, key, ENV_VAR_KEY)
            )
            return default_value
    else:
        return default_value


DEFAULT: Munch = munchify(
    {
        "CSV_DATA_DIR": from_env("CSV_DATA_DIR", "~/data"),
        "OUTPUT_DIR": from_env("OUTPUT_DIR", "~/out"),
    }
)


TEST: Munch = munchify({"CSV_DATA_DIR": "data", "OUTPUT_DIR": "out"})


def from_file(fname: str = DEFAULT_CONFIG_FILENAME, testing: bool = False) -> Munch:
    if testing:
        return TEST
    try:
        with open(os.path.expanduser(fname)) as fd:
            conf = yaml.safe_load(fd)
        conf: Munch = munchify(conf)
        return conf
    except IOError:
        print("A configuration file named '%s' is missing" % fname)
        s_conf = yaml.dump(
            unmunchify(DEFAULT),
            explicit_start=True,
            indent=True,
            default_flow_style=False,
        )
        print(
            """
Creating this file

%s

You still have to create directories with data and put your data in!
"""
            % s_conf
        )
        time.sleep(3)
        try:
            with open(os.path.expanduser(fname), "w") as fd:
                fd.write(s_conf)
        except IOError:
            print("Can create '%s'" % fname)
    print("Trying anyway with default configuration")
    return DEFAULT
