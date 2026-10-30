"""Validate ATX changelog sections delimited by level 2 headings.
Level 3 and below are free-form content and cannot end a section.
Fenced code is content, not document headings.
"""
import argparse
import json
from pathlib import Path
import re


def require(condition, message):
    if not condition:
        raise SystemExit(message)


def markdown_lines(text):
    fence = None
    for line in text.splitlines(keepends=True):
        marker = re.match(r'^ {0,3}(`{3,}|~{3,})(.*)$', line)
        if fence:
            closing = marker and marker[1][0] == fence[0] and len(marker[1]) >= fence[1] and not marker[2].strip()
            yield line, 'fence' if closing else 'code'
            if closing:
                fence = None
        elif marker and (marker[1][0] == '~' or '`' not in marker[2]):
            fence = (marker[1][0], len(marker[1]))
            yield line, 'fence'
        else:
            yield line, 'text'


parser = argparse.ArgumentParser()
parser.add_argument('--tag')
parser.add_argument('--notes', type=Path)
args = parser.parse_args()
if (args.tag is None) != (args.notes is None):
    parser.error('Release tag and notes path must be supplied together')

number = r'(?:0|[1-9][0-9]*)'
identifier = r'(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)'
semver = rf'{number}\.{number}\.{number}(?:-{identifier}(?:\.{identifier})*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?'
version = json.loads(Path('package.json').read_text()).get('version')
require(isinstance(version, str) and re.fullmatch(semver, version), 'Invalid package version')
text = Path('CHANGELOG.md').read_text(encoding='utf-8-sig')
structure = ''.join(line if kind == 'text' else re.sub(r'[^\r\n]', ' ', line) for line, kind in markdown_lines(text))
require(len(re.findall(r'^ {0,3}#(?:[ \t]|$)', structure, re.M)) <= 1, 'Changelog has more than one level 1 heading')
sections = list(re.finditer(r'^ {0,3}##(?:[ \t]+(.*)|$)', structure, re.M))
headings = []
unreleased = []
for section in sections:
    label = (section[1] or '').strip()
    heading = re.fullmatch(rf'\[({semver})\](?:[ \t].*)?', label)
    if heading:
        headings.append((section, heading[1]))
    elif label == '[Unreleased]':
        unreleased.append(section)
    else:
        raise SystemExit('Unrecognized level 2 changelog heading: ' + label)

require(headings, 'Changelog has no versioned heading')
heading, first_version = headings[0]
require(len(unreleased) <= 1 and (not unreleased or unreleased[0].start() == sections[0].start() < heading.start()), 'Unreleased section must be unique and first')


def has_entries(body):
    return any(line.strip() and kind != 'fence' and (kind == 'code' or not re.match(r'^ {0,3}#{1,6}(?:[ \t]|$)', line)) for line, kind in markdown_lines(body))


if args.tag is not None and unreleased:
    require(not has_entries(text[unreleased[0].end():heading.start()]), 'Unreleased section has entries')
versions = [value for _, value in headings]
require(len(set(versions)) == len(versions), 'Duplicate changelog version')
require(first_version == version and (args.tag is None or args.tag == 'v' + version), 'Tag, package and changelog head must agree')
end = headings[1][0].start() if len(headings) > 1 else len(text)
require(has_entries(text[heading.end():end]), 'Release notes contain no entries')
if args.notes is not None:
    args.notes.write_text(text[heading.start():end].rstrip() + '\n')
print(f'Changelog checks passed for {version}.')
