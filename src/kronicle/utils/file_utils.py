from configparser import ConfigParser, ExtendedInterpolation
from hashlib import md5, sha256, sha512
from json import dump, load
from os import makedirs, stat
from os.path import abspath, exists, expanduser
from pathlib import Path
from time import time
from typing import Literal


def is_dir(dir_local_path: str):
    return Path(dir_local_path).is_dir()


def check_is_dir(dir_local_path: str, err_msg: str | None = None):
    if not is_dir(dir_local_path):
        raise FileNotFoundError(err_msg if err_msg is not None else f"No such file: '{dir_local_path}'")
    return dir_local_path


def make_dir(dir_local_path: str):
    if not exists(dir_local_path):
        makedirs(dir_local_path)
    else:
        check_is_dir(dir_local_path)
    return dir_local_path


def exists_file(file_local_path: str):
    return exists(file_local_path)


def is_file(file_local_path: str):
    return Path(file_local_path).is_file()


def check_is_file(file_local_path: str, err_msg: str | None = None):
    if not is_file(file_local_path):
        raise FileNotFoundError(err_msg if err_msg is not None else f"No such file: '{file_local_path}'")
    return file_local_path


def expand_file_path(file_local_path: str):
    local_path = expanduser(file_local_path)  # expand '~'
    local_path = abspath(local_path)  # make it absolute
    return local_path


def get_file_size(file_local_path: str) -> int:
    """
    Returns a file size in bytes
    :param file_local_path:
    :return:
    """
    return stat(file_local_path).st_size


# # @decorator_timer
# def get_file_extension(file_local_path: str) -> str:
#     """
#     Returns a file size in bytes
#     :param file_local_path:
#     :return:
#     """
#     ext = "".join(Path(file_local_path).suffixes)
#     if FileExtensions.get(ext) is not None:
#         return ext
#     recognized_ext = [key for key in FileExtensions.keys() if ext.endswith(key)]
#     return recognized_ext[-1] if len(recognized_ext) else ext


# def get_file_mime(file_local_path: str) -> str:
#     """
#     Returns the MIME type of a file
#     (Based on 'puremagic' lib https://pypi.org/project/puremagic)
#     :param file_local_path:
#     :return:
#     """
#     here = "get_file_mime"
#     file_info = magic_file(file_local_path)
#     if not file_info:  # pragma: no cover
#         log_d(here, "magic_file failed, no file_info for", file_local_path)
#         if get_file_extension(file_local_path) == ".csv":
#             return "text/csv"
#         return "application/octet-stream"
#     if get_file_extension(file_local_path) == ".geojson":
#         return "application/geo+json"

#     mime_type = file_info[0].mime_type
#     if not mime_type:
#         log_d(here, "file_info no mime_type", file_info)
#         if get_file_extension(file_local_path) == ".csv":
#             return "text/csv"
#         return "application/octet-stream"

#     if mime_type == "application/x-gzip":  # pragma: no cover
#         return "application/gzip"
#     if mime_type == "text/spreadsheet":
#         if file_local_path.endswith(".xls"):  # pragma: no cover
#             return "application/vnd.ms-excel"
#         else:
#             log_d(here, "mime_type", mime_type)
#             log_d(here, "file_local_path", file_local_path)
#             log_d(here, "file_extension", get_file_extension(file_local_path))
#             return "application/vnd.ms-excel"

#     if mime_type.endswith("yaml"):
#         return "text/x-yaml"

#     return mime_type


# def get_file_charset(file_local_path: str):
#     """
#     Returns the encoding of a file
#     (Uses the library 'chardet': https://pypi.org/project/chardet)
#     :param file_local_path: the path of a local file
#     :return: the encoding of the file
#     """
#     mime_type = get_file_mime(file_local_path)
#     if not mime_type.startswith("text"):
#         # not detecting application/* as 'utf-8' is the norm
#         # and mime_type not in ['application/x-yaml', 'application/json', 'application/geo+json', 'application/xml',
#         # 'application/javascript']
#         return None
#     with open(file_local_path, "rb") as file_stream:
#         data = file_stream.read()
#         charset = detect(data, True)["encoding"]
#         return charset


ACCEPTED_HASH_ALGOS = ("MD5", "SHA-256", "SHA256", "SHA-512", "SHA512")


def get_file_hash(file_local_path: str, hash_algo: str = "md5") -> str:
    file_content = open(check_is_file(file_local_path), "rb").read()
    if not isinstance(hash_algo, str) or (upper_algo := hash_algo.upper()) not in ACCEPTED_HASH_ALGOS:
        raise ValueError(f"Hash algorithm should be MD5, SHA-256 or SHA-512, got: '{hash_algo}'")
    if upper_algo == "MD5":
        return md5(file_content).hexdigest()
    if upper_algo in ("SHA256", "SHA-256"):
        return sha256(file_content).hexdigest()
    # if upper_algo not in ("SHA512", "SHA-512"):
    #      raise NotImplementedError("This hash algorithm was not recognized: {hash_algo}")
    return sha512(file_content).hexdigest()


def read_json_file(file_path, mode: Literal["b", "t"] = "t"):  # pragma: no cover
    check_is_file(file_path)
    with open(file_path, f"r{mode}") as json_file_content:
        return load(json_file_content)


def read_ini_conf(file_path):
    check_is_file(file_path)
    config_reader = ConfigParser(interpolation=ExtendedInterpolation())
    config_reader.read(file_path)
    return config_reader


def write_file(destination_file_path: str, content, mode: Literal["b", "t"] = "t"):  # pragma: no cover
    """

    :param destination_file_path: the path of the file
    :param content:
    :param mode: use 'b'for binary mode, 't' for text mode (default)
    :return:
    """
    with open(destination_file_path, f"w{mode}") as file:
        file.write(content)


def write_json_file(destination_file_path: str, json_dict):  # pragma: no cover
    with open(destination_file_path, "w") as file:
        dump(json_dict, file, ensure_ascii=False)


# class FileDetails(Serializable):
#     def __init__(self, file_local_path: str):
#         check_is_file(file_local_path)
#         self.path: str = file_local_path
#         self.name: str = Path(file_local_path).name
#         self.extension: str = get_file_extension(file_local_path)
#         self.mime: str = get_file_mime(file_local_path)
#         self.charset: str | None = get_file_charset(self.path) if self.mime.startswith("text") else None
#         self.size: int = get_file_size(file_local_path)
#         self.md5: str = get_file_hash(file_local_path)

#     @staticmethod
#     def from_json(o):
#         pass


if __name__ == "__main__":  # pragma: no cover
    tests = "FileUtils"
    begin = time()

    # test_file_dir = "../tests/_test_files/"
    # yaml_file_path = test_file_dir + "RUDI producer internal API - 1.3.0.yml"
    # right_path = test_file_dir + "unicorn.png"
    # bin_file = test_file_dir + "WERTSTOFFE.m8s"
    # tar_gz_file = test_file_dir + "rudi-node-read.tar.gz"
    # txt_file = test_file_dir + "RUDI producer internal API - 1.3.0.yml"
    # csv_file = test_file_dir + "dummy.csv"
    # wrong_path = test_file_dir + "toto"
    # unicode_file_path = test_file_dir + "unicode_chars.txt"
    # write_file(unicode_file_path, "tut0156êµîƒﬁÌπÏ“{ëôøÇ¡¶{µœ≤é≤")
    ini_file = "../.conf/config.ini"
    conf = read_ini_conf(ini_file)
    print("url:", conf.get("db", "DB_URL"))
