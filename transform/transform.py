import math
from abc import ABC, abstractmethod
from typing import TextIO

BYTES = 28012696901
LINES = 114608388
# 28012696901 bytes, 114608388 lines, 244 bytes per line on average
PROGRESS_BAR = 50
INTERVALS = 1000
BATCHES = 200
MAX_IRREGULAR = 0

last_printed_perc = 0
irregular_left = MAX_IRREGULAR


class LogLine:
    def __init__(self, date: str, severity: str, cxx: str, body: str, raw: str):
        self.date = date
        self.severity = severity
        self.cxx = cxx
        self.body = body
        self.raw = raw

    def __str__(self):
        return self.raw


class LineConsumer(ABC):
    @abstractmethod
    def consume(self, line: LogLine):
        pass

    def finalise(self):
        pass


class VerbCounter(LineConsumer):
    verbs = {}

    @staticmethod
    def find_verb(body: str) -> str | None:
        fallback = '__NO_VERB_FOUND'
        for w in body.lower().split():
            if w.endswith('ing'):
                return w
            elif w.endswith('ed') and w.isalpha():
                fallback = w.replace('ed', 'ing')
        return fallback

    def consume(self, line: LogLine):
        verb = self.find_verb(line.body)
        if verb in self.verbs:
            self.verbs[verb] += 1
        else:
            self.verbs[verb] = 1

    def __str__(self):
        return '\n'.join([f'{k}: {self.verbs[k]}' for k in sorted(self.verbs.keys())])


class CxxCounter(LineConsumer):
    # 99.72% log records are CBS
    cs = {}

    def consume(self, line: LogLine):
        global irregular_left
        cxx: str = line.cxx.upper()
        if irregular_left > 0 and (len(cxx) != 3 or not cxx.isalpha()):
            irregular_left -= 1
            print(line)
        if cxx in self.cs:
            self.cs[cxx] += 1
        else:
            self.cs[cxx] = 1

    def __str__(self):
        return str(self.cs)


class Batcher(LineConsumer):
    index = 1
    lines_in_current = 0
    current: TextIO | None = None

    def __init__(self, prefix: str, max_lines: int):
        self.prefix = prefix
        self.max_lines = max_lines
        self.change_file()

    def change_file(self):
        if self.current is not None:
            self.current.write('\n')
            self.current.close()
            self.index += 1
        self.current = open(f'{self.prefix}_{self.index}.csv', 'w')
        self.lines_in_current = 0

    def consume(self, line: LogLine):
        if self.lines_in_current >= self.max_lines:
            self.change_file()
        clean_body = line.body.replace("\"", "")
        self.current.write(f'{line.date.strip(",")},{line.severity},{line.cxx},"{clean_body}"\n')
        self.lines_in_current += 1

    def finalise(self):
        if self.current is not None:
            self.current.close()

    def __str__(self):
        return f'Batched data into {self.index} files'


##################################

def split_line(raw_line: str) -> LogLine | None:
    words = raw_line.strip('\uFEFF').split()
    try:
        return LogLine(
            date=f'{words[0]} {words[1]}',
            severity=words[2],
            cxx=words[3],
            body=' '.join(words[4:]),
            raw=raw_line,
        )
    except IndexError:
        return None


def display_progress(processed_lines: int):
    global last_printed_perc
    perc: float = processed_lines / LINES
    perc100: int = math.floor(perc * INTERVALS)
    if perc100 > last_printed_perc:
        last_printed_perc = perc100
        perc5: int = math.floor(perc * PROGRESS_BAR)
        bar: str = '#' * perc5 + ' ' * (PROGRESS_BAR - perc5)
        whole: str = f'\r[{bar}] {processed_lines}/{LINES}'
        print(whole, end='')


def main(log_path: str):
    consumers: list[LineConsumer] = [
        VerbCounter(),
        CxxCounter(),
        Batcher('batch/log', math.ceil(LINES / BATCHES)),
    ]
    if len(consumers) > 0:
        with open(log_path, 'r') as f:
            processed = 0
            invalid = 0
            for raw_line in f:
                processed += 1
                line = split_line(raw_line)
                if line is None:
                    invalid += 1
                    continue
                for cons in consumers:
                    cons.consume(line)
                display_progress(processed)
        print()
        for cons in consumers:
            print(str(cons))
            cons.finalise()


main('source/Windows.log')
