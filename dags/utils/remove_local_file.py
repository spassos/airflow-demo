import glob
import os


def fn_remove_local_file(filepath):
    files = glob.glob(filepath)
    for f in files:
        os.remove(f)
