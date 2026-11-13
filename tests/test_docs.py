"""Documentation integrity: relative links resolve and ADRs are well-formed."""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DOCS = sorted([*ROOT.glob("*.md"), *(ROOT / "docs").rglob("*.md")])
ADR_DIR = ROOT / "docs" / "adr"
ADRS = sorted(ADR_DIR.glob("[0-9][0-9][0-9][0-9]-*.md"))

LINK = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")
FENCE = re.compile(r"```.*?```", re.S)
REQUIRED_SECTIONS = ["## Context", "## Options considered", "## Decision", "## Consequences"]
STATUS = re.compile(r"^- \*\*Status:\*\* (Proposed|Accepted|Superseded by ADR-\d{4})\s*$", re.M)
DATE = re.compile(r"^- \*\*Date:\*\* \d{4}-\d{2}-\d{2}", re.M)


def _links(md: Path) -> list[str]:
    text = FENCE.sub("", md.read_text())  # ignore links inside code examples
    return LINK.findall(text)


@pytest.mark.parametrize("md", DOCS, ids=lambda p: str(p.relative_to(ROOT)))
def test_relative_links_resolve(md):
    broken = []
    for target in _links(md):
        if re.match(r"^[a-z]+:", target) or target.startswith("#"):
            continue  # external URL / in-page anchor
        path = (md.parent / target.split("#", 1)[0]).resolve()
        if not path.exists():
            broken.append(target)
    assert not broken, f"{md.relative_to(ROOT)} has broken links: {broken}"


def test_there_is_at_least_one_adr():
    assert ADRS, "no ADRs found in docs/adr"


@pytest.mark.parametrize("adr", ADRS, ids=lambda p: p.name)
def test_adr_structure(adr):
    text = adr.read_text()
    number = adr.name[:4]
    assert text.startswith(f"# ADR-{number}: "), "title must be '# ADR-NNNN: ...'"
    assert STATUS.search(text), "missing or invalid '- **Status:**' line"
    assert DATE.search(text), "missing '- **Date:** YYYY-MM-DD' line"
    positions = [text.find(s) for s in REQUIRED_SECTIONS]
    assert all(p >= 0 for p in positions), f"missing sections: {REQUIRED_SECTIONS}"
    assert positions == sorted(positions), "sections out of order"


def test_adr_numbers_are_sequential_and_unique():
    numbers = [int(p.name[:4]) for p in ADRS]
    assert numbers == list(range(1, len(numbers) + 1))


def test_adr_index_lists_every_adr():
    index = (ADR_DIR / "README.md").read_text()
    for adr in ADRS:
        assert f"]({adr.name})" in index, f"{adr.name} missing from docs/adr/README.md"
    for adr in ADRS:
        status = STATUS.search(adr.read_text()).group(1)
        row = next(line for line in index.splitlines() if f"]({adr.name})" in line)
        assert row.rstrip(" |").endswith(status), f"index status for {adr.name} is stale"


def test_superseded_adrs_point_to_existing_adr():
    numbers = {p.name[:4] for p in ADRS}
    for adr in ADRS:
        m = re.search(r"Superseded by ADR-(\d{4})", adr.read_text())
        if m:
            assert m.group(1) in numbers
