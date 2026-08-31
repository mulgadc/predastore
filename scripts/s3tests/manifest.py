"""Compare two ceph/s3-tests conformance manifests.

A manifest is one line per test case, `STATUS|node id`, sorted. The recorder
half lives in predastore_cleanup.py, which writes it from pytest's own report;
that same file also decides what gets deselected, and never deselects a case
the committed baseline records as PASS.

The interesting question a run answers is not the pass rate -- predastore fails
a lot of s3-tests and will for a while -- but which lines moved.

Usage:
    manifest.py merge <run.txt> <skips.txt> <out.txt>
    manifest.py compare <baseline.txt> <current.txt> [--strict]
"""

import sys

PASS = 'PASS'
FAIL = 'FAIL'
ERROR = 'ERROR'
SKIP = 'SKIP'


def read(path):
    entries = {}
    with open(path, encoding='utf-8') as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            status, _, node = line.partition('|')
            if node:
                entries[node] = status
    return entries


def read_skips(path):
    """Node id lines from s3-tests-skips.txt: `node id  # reason`.

    Marker lines are skipped here, and so is the actual deselecting: both now
    happen in pytest_collection_modifyitems in predastore_cleanup.py, which
    also drops any case the committed baseline records as PASS before it
    reaches --deselect. This is left only for the stale-name check below.
    """
    skips = {}
    with open(path, encoding='utf-8') as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith('#') or line.startswith('marker:'):
                continue
            node, _, reason = line.partition('#')
            node = node.strip()
            if node:
                skips[node] = reason.strip()
    return skips


def merge(run_path, skips_path, out_path):
    """Write the manifest from a run, whose SKIP status is already final."""
    entries = read(run_path)
    skips = read_skips(skips_path)

    with open(out_path, 'w', encoding='utf-8') as out:
        out.write('# ceph/s3-tests conformance manifest for predastore.\n')
        out.write('# Regenerate with `make s3-tests-baseline`. A regression is\n')
        out.write('# a line that moved off PASS, or a case that vanished.\n')
        for node in sorted(entries):
            out.write('%s|%s\n' % (entries[node], node))

    # A skip that names a case the suite no longer has is a stale exclusion,
    # and it hides the fact that nothing is being excluded any more.
    stale = sorted(n for n in skips if n not in entries)
    for node in stale:
        print('skip names a case this run did not collect: %s' % node, file=sys.stderr)
    return 0


def compare(baseline_path, current_path, strict):
    baseline = read(baseline_path)
    current = read(current_path)

    regressed = sorted(n for n, s in current.items()
                       if baseline.get(n) == PASS and s != PASS)
    vanished = sorted(n for n in baseline if n not in current)
    fixed = sorted(n for n, s in current.items()
                   if n in baseline and baseline[n] != PASS and s == PASS)
    new = sorted(n for n in current if n not in baseline)
    failing = sorted(n for n, s in current.items() if s in (FAIL, ERROR))

    def show(title, nodes, limit=40):
        if not nodes:
            return
        print('\n%s (%d)' % (title, len(nodes)))
        for node in nodes[:limit]:
            print('  %s' % node)
        if len(nodes) > limit:
            print('  ... and %d more' % (len(nodes) - limit))

    print('%d cases: %d pass, %d fail, %d error, %d skip' % (
        len(current),
        sum(1 for s in current.values() if s == PASS),
        sum(1 for s in current.values() if s == FAIL),
        sum(1 for s in current.values() if s == ERROR),
        sum(1 for s in current.values() if s == SKIP)))

    show('REGRESSED -- these passed in the baseline', regressed)
    show('VANISHED -- in the baseline, not in this run', vanished)
    show('FIXED -- re-record the baseline in the same change', fixed)
    show('NEW -- not in the baseline', new)

    # A regression and a vanished case are both failures of the comparison
    # itself. Everything else is reported and does not fail the run, because a
    # branch must not be red for gaps it did not introduce.
    status = 0
    if regressed or vanished:
        status = 1
    if strict and failing:
        show('FAILING -- strict mode fails on any of these', failing)
        status = 1
    return status


def main(argv):
    if len(argv) >= 5 and argv[1] == 'merge':
        return merge(argv[2], argv[3], argv[4])
    if len(argv) >= 4 and argv[1] == 'compare':
        return compare(argv[2], argv[3], '--strict' in argv[4:])
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == '__main__':
    sys.exit(main(sys.argv))
