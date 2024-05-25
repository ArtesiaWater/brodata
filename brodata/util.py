from io import StringIO
import requests


class FileOrUrl:
    def __init__(self, fname, timeout=5):
        if fname.startswith("http"):
            r = requests.get(fname, timeout=timeout)
            if not r.ok:
                raise (Exception((f"Retieving data from {fname} failed")))
            self._read_contents(StringIO(r.text))
        else:
            with open(fname, "r") as f:
                self._read_contents(f)
