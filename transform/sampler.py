import math

BPL = 244.5  # bytes per line
SAMPLE_MB = 100
BATCHES = 200
BATCH_TEMPLATE = 'batch/log_#.csv'
OUTPUT = 'sample/sample_100MB.csv'

SAMPLE_B = SAMPLE_MB * 1024 * 1024
LPF = (SAMPLE_B / BPL) / BATCHES  # lines per file


def main():
    lines_per_batch = math.ceil(LPF)
    print(f'Will generate {lines_per_batch} lines per batch, '
          f'{lines_per_batch * BATCHES} lines in total. ENTER to continue')
    input()
    with open(OUTPUT, 'w') as fout:
        for i in range(1, BATCHES + 1):
            with open(BATCH_TEMPLATE.replace('#', str(i))) as fin:
                lines_left = lines_per_batch
                for line in fin:
                    fout.write(line)
                    lines_left -= 1
                    if lines_left <= 0:
                        break


main()
