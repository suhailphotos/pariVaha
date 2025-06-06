# parivaha.progress

from contextlib import contextmanager
import click
try:
    from tqdm import tqdm as _tqdm
except ModuleNotFoundError:
    _tqdm = None

BAR_FORMAT = "{l_bar}{bar}| {n_fmt}/{total_fmt} • {rate_fmt}{postfix}"

@contextmanager
def progress(total: int, desc: str):
    if _tqdm:
        bar = _tqdm(total=total, desc=desc, unit="pg",
                    bar_format=BAR_FORMAT, colour="green")
        yield bar
        bar.close()
    else:
        with click.progressbar(length=total, label=desc) as bar:
            yield bar
